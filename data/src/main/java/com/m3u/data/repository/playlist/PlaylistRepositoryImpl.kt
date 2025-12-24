package com.m3u.data.repository.playlist

import android.content.ContentResolver
import android.content.Context
import android.net.Uri
import androidx.core.net.toUri
import androidx.work.WorkManager
import com.m3u.core.architecture.preferences.PlaylistStrategy
import com.m3u.core.architecture.preferences.PreferencesKeys
import com.m3u.core.architecture.preferences.Settings
import com.m3u.core.architecture.preferences.get
import com.m3u.core.util.basic.startsWithAny
import com.m3u.core.util.copyToFile
import com.m3u.core.util.readFileName
import com.m3u.data.api.OkhttpClient
import com.m3u.data.database.dao.ChannelDao
import com.m3u.data.database.dao.PlaylistDao
import com.m3u.data.database.dao.ProgrammeDao
import com.m3u.data.database.model.Channel
import com.m3u.data.database.model.DataSource
import com.m3u.data.database.model.Playlist
import com.m3u.data.database.model.PlaylistWithChannels
import com.m3u.data.database.model.fromLocal
import com.m3u.data.database.model.toMap
import com.m3u.data.parser.m3u.M3UData
import com.m3u.data.parser.m3u.M3UParser
import com.m3u.data.parser.m3u.toChannel
import com.m3u.data.parser.xtream.XtreamChannelInfo
import com.m3u.data.parser.xtream.XtreamInput
import com.m3u.data.parser.xtream.XtreamLive
import com.m3u.data.parser.xtream.XtreamParser
import com.m3u.data.parser.xtream.XtreamSerial
import com.m3u.data.parser.xtream.XtreamVod
import com.m3u.data.parser.xtream.asChannel
import com.m3u.data.parser.xtream.toChannel
import com.m3u.data.repository.BackupOrRestoreContracts
import com.m3u.data.repository.createCoroutineCache
import com.m3u.data.worker.SubscriptionWorker
import dagger.hilt.android.qualifiers.ApplicationContext
import io.ktor.http.Url
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.InputStream
import java.io.Reader
import javax.inject.Inject

private const val BUFFER_M3U_CAPACITY = 500
private const val BUFFER_XTREAM_CAPACITY = 100
private const val BUFFER_RESTORE_CAPACITY = 400

internal class PlaylistRepositoryImpl @Inject constructor(
    private val playlistDao: PlaylistDao,
    private val channelDao: ChannelDao,
    private val programmeDao: ProgrammeDao,
    @OkhttpClient(true) private val okHttpClient: OkHttpClient,
    private val m3uParser: M3UParser,
    private val xtreamParser: XtreamParser,
    private val workManager: WorkManager,
    @ApplicationContext private val context: Context,
    private val settings: Settings
) : PlaylistRepository {

    override suspend fun m3uOrThrow(
        title: String,
        url: String,
        callback: (count: Int) -> Unit
    ) {
        val internalUrl = url.copyToInternalDirPath()
        
        // 1. 预解析阶段：先将数据下载并解析到内存中，不操作数据库
        val validChannels = mutableListOf<Channel>()
        
        try {
            val inputStream = when {
                url.isSupportedNetworkUrl() -> openNetworkInput(internalUrl)
                url.isSupportedAndroidUrl() -> openAndroidInput(internalUrl)
                else -> null
            } ?: throw IllegalArgumentException("Unsupported URL or cannot open stream: $internalUrl")

            inputStream.use { input ->
                m3uParser.parse(input.buffered())
                    .collect { m3uData ->
                        // 这里暂时不做复杂的M3U格式修改，仅做基础收集
                        // 稍后根据你的新思路，可以在这里加过滤逻辑
                        validChannels.add(m3uData.toChannel(internalUrl))
                        
                        // 回调进度仅用于UI展示，不代表写入数据库
                        callback(validChannels.size)
                    }
            }
        } catch (e: Exception) {
            // 如果下载或解析失败，直接抛出异常。
            // 此时数据库旧数据还没有被删除，保证了数据安全。
            throw e
        }

        if (validChannels.isEmpty()) {
            throw RuntimeException("No channels found in the playlist.")
        }

        // ============================================================
        // 【核心修改】合并旧的收藏状态
        // ============================================================
        // 1. 从数据库中获取旧的频道列表
        val oldChannels = channelDao.getByPlaylistUrl(internalUrl)
        
        // 2. 提取出被标记为“喜欢”的频道 URL 集合
        val favoriteUrls = oldChannels
            .filter { it.favourite }
            .map { it.url }
            .toSet()

        // 3. 遍历新频道，如果 URL 命中收藏集合，恢复收藏状态
        if (favoriteUrls.isNotEmpty()) {
            validChannels.forEach { channel ->
                if (channel.url in favoriteUrls) {
                    channel.favourite = true
                }
            }
        }
        // ============================================================

        // 2. 数据库写入阶段
        val playlistStrategy = settings[PreferencesKeys.PLAYLIST_STRATEGY]
        val favOrHiddenRelationIds = when (playlistStrategy) {
            PlaylistStrategy.ALL -> emptyList()
            else -> channelDao.getFavOrHiddenRelationIdsByPlaylistUrl(url)
        }
        val favOrHiddenUrls = when (playlistStrategy) {
            PlaylistStrategy.ALL -> emptyList()
            else -> channelDao.getFavOrHiddenUrlsByPlaylistUrlNotContainsRelationId(url)
        }

        // 删除旧数据
        when (playlistStrategy) {
            PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(url)
            PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(url)
        }

        val playlist = playlistDao.get(internalUrl)?.copy(
            title = title,
            source = DataSource.M3U
        ) ?: Playlist(title, internalUrl, source = DataSource.M3U)
        playlistDao.insertOrReplace(playlist)

        // 过滤并批量插入新数据
        val finalChannels = validChannels.filterNot { channel ->
            val relationId = channel.relationId
            when {
                relationId == null || relationId.isBlank() -> channel.url in favOrHiddenUrls
                else -> relationId in favOrHiddenRelationIds
            }
        }

        finalChannels.chunked(BUFFER_M3U_CAPACITY).forEach { batch ->
            channelDao.insertOrReplaceAll(*batch.toTypedArray())
        }
    }

    override suspend fun xtreamOrThrow(
        title: String,
        basicUrl: String,
        username: String,
        password: String,
        type: String?,
        callback: (count: Int) -> Unit
    ): Unit = withContext(Dispatchers.IO) {
        val input = XtreamInput(basicUrl, username, password, type)
        val (
            liveCategories,
            vodCategories,
            serialCategories,
            allowedOutputFormats,
            serverProtocol,
            port
        ) = xtreamParser.getXtreamOutput(input)

        val liveContainerExtension = if ("ts" in allowedOutputFormats) "ts"
        else allowedOutputFormats.firstOrNull() ?: "ts"

        val livePlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_LIVE),
            serverProtocol = serverProtocol,
            port = port
        ).let { url ->
            playlistDao.get(url)
                ?.takeIf { it.source == DataSource.Xtream }
                ?.copy(title = title)
                ?: Playlist(title = title, url = url, source = DataSource.Xtream)
        }
        val vodPlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_VOD),
            serverProtocol = serverProtocol,
            port = port
        ).let { url ->
            playlistDao.get(url)
                ?.takeIf { it.source == DataSource.Xtream }
                ?.copy(title = title)
                ?: Playlist(title = title, url = url, source = DataSource.Xtream)
        }
        val seriesPlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_SERIES),
            serverProtocol = serverProtocol,
            port = port
        ).let { url ->
            playlistDao.get(url)
                ?.takeIf { it.source == DataSource.Xtream }
                ?.copy(title = title)
                ?: Playlist(title = title, url = url, source = DataSource.Xtream)
        }

        val favOrHiddenRelationIds = channelDao.getFavOrHiddenRelationIdsByPlaylistUrl(
            livePlaylist.url,
            vodPlaylist.url,
            seriesPlaylist.url
        )

        val requiredLives = type == null || type == DataSource.Xtream.TYPE_LIVE
        val requiredVods = type == null || type == DataSource.Xtream.TYPE_VOD
        val requiredSeries = type == null || type == DataSource.Xtream.TYPE_SERIES

        val playlistStrategy = settings[PreferencesKeys.PLAYLIST_STRATEGY]

        if (requiredLives) {
            when (playlistStrategy) {
                PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(livePlaylist.url)
                PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(livePlaylist.url)
            }
            playlistDao.insertOrReplace(livePlaylist)
        }
        if (requiredVods) {
            when (playlistStrategy) {
                PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(vodPlaylist.url)
                PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(vodPlaylist.url)
            }
            playlistDao.insertOrReplace(vodPlaylist)
        }
        if (requiredSeries) {
            when (playlistStrategy) {
                PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(seriesPlaylist.url)
                PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(seriesPlaylist.url)
            }
            playlistDao.insertOrReplace(seriesPlaylist)
        }

        var currentCount = 0
        callback(currentCount)

        val cache = createCoroutineCache(BUFFER_XTREAM_CAPACITY) { all ->
            currentCount += all.size
            callback(currentCount)
            channelDao.insertOrReplaceAll(*all.toTypedArray())
        }

        xtreamParser
            .parse(input)
            .mapNotNull { current ->
                when (current) {
                    is XtreamLive -> {
                        val favOrHidden = with(current.streamId) {
                            val relationId = this.toString()
                            this != null && relationId in favOrHiddenRelationIds
                        }
                        if (favOrHidden) return@mapNotNull null
                        current.toChannel(
                            basicUrl = basicUrl,
                            username = username,
                            password = password,
                            playlistUrl = livePlaylist.url,
                            category = liveCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty(),
                            containerExtension = liveContainerExtension
                        )
                    }

                    is XtreamVod -> {
                        val favOrHidden = with(current.streamId) {
                            val relationId = this.toString()
                            this != null && relationId in favOrHiddenRelationIds
                        }
                        if (favOrHidden) return@mapNotNull null
                        current.toChannel(
                            basicUrl = basicUrl,
                            username = username,
                            password = password,
                            playlistUrl = vodPlaylist.url,
                            category = vodCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty()
                        )
                    }

                    is XtreamSerial -> {
                        val favOrHidden = with(current.seriesId) {
                            val relationId = this.toString()
                            this != null && relationId in favOrHiddenRelationIds
                        }
                        if (favOrHidden) return@mapNotNull null
                        current.asChannel(
                            basicUrl = basicUrl,
                            username = username,
                            password = password,
                            playlistUrl = seriesPlaylist.url,
                            category = serialCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty()
                        )
                    }
                }
            }
            .onEach(cache::push)
            .onCompletion { cache.flush() }
            .collect()
    }

    override suspend fun insertEpgAsPlaylist(title: String, epg: String) {
        playlistDao.insertOrReplace(
            Playlist(title = title, url = epg, source = DataSource.EPG)
        )
    }

    override suspend fun refresh(url: String) {
        val playlist = get(url) ?: return
        // 修正：使用 fromLocal 替代 refreshable，避免编译错误
        if (playlist.fromLocal) {
            return
        }

        when (playlist.source) {
            DataSource.M3U -> {
                SubscriptionWorker.m3u(workManager, playlist.title, url)
            }

            DataSource.EPG -> {
                SubscriptionWorker.epg(workManager, url, true)
            }

            DataSource.Xtream -> {
                val xtreamInput = XtreamInput.decodeFromPlaylistUrl(url)
                SubscriptionWorker.xtream(
                    workManager = workManager,
                    title = playlist.title,
                    url = url,
                    basicUrl = xtreamInput.basicUrl,
                    username = xtreamInput.username,
                    password = xtreamInput.password
                )
            }

            else -> throw IllegalStateException("Refresh data source ${playlist.source} is unsupported currently.")
        }
    }

    override suspend fun backupOrThrow(uri: Uri): Unit = withContext(Dispatchers.IO) {
        val json = Json { prettyPrint = false }
        val all = playlistDao.getAllWithChannels()
        context.contentResolver.openOutputStream(uri)?.use {
            val writer = it.bufferedWriter()
            all.forEach { (playlist, channels) ->
                val encodedPlaylist = json.encodeToString(playlist)
                val wrappedPlaylist = BackupOrRestoreContracts.wrapPlaylist(encodedPlaylist)
                writer.appendLine(wrappedPlaylist)

                channels.forEach { channel ->
                    val encodedChannel = json.encodeToString(channel)
                    val wrappedChannel = BackupOrRestoreContracts.wrapChannel(encodedChannel)
                    writer.appendLine(wrappedChannel)
                }
            }
            writer.flush()
        }
    }

    override suspend fun restoreOrThrow(uri: Uri): Unit = withContext(Dispatchers.IO) {
        val json = Json { ignoreUnknownKeys = true }
        val mutex = Mutex()
        context.contentResolver.openInputStream(uri)?.use {
            val reader = it.bufferedReader()

            val channels = mutableListOf<Channel>()
            reader.forEachLine { line ->
                if (line.isBlank()) return@forEachLine
                val encodedPlaylist = BackupOrRestoreContracts.unwrapPlaylist(line)
                val encodedChannel = BackupOrRestoreContracts.unwrapChannel(line)
                when {
                    encodedPlaylist != null -> {
                        val playlist = json.decodeFromString<Playlist>(encodedPlaylist)
                        playlistDao.insertOrReplace(playlist)
                    }

                    encodedChannel != null -> {
                        val channel = json.decodeFromString<Channel>(encodedChannel)
                        channels.add(channel)
                        if (channels.size >= BUFFER_RESTORE_CAPACITY) {
                            mutex.withLock {
                                if (channels.size >= BUFFER_RESTORE_CAPACITY) {
                                    channelDao.insertOrReplaceAll(*channels.toTypedArray())
                                    channels.clear()
                                }
                            }
                        }
                    }

                    else -> {}
                }
            }
            mutex.withLock {
                channelDao.insertOrReplaceAll(*channels.toTypedArray())
            }
        }
    }

    override suspend fun pinOrUnpinCategory(url: String, category: String) {
        playlistDao.updatePinnedCategories(url) { prev ->
            if (category in prev) prev - category
            else prev + category
        }
    }

    override suspend fun hideOrUnhideCategory(url: String, category: String) {
        playlistDao.hideOrUnhideCategory(url, category)
    }

    override fun observeAll(): Flow<List<Playlist>> = playlistDao
        .observeAll()
        .catch { emit(emptyList()) }

    override fun observeAllEpgs(): Flow<List<Playlist>> = playlistDao
        .observeAllEpgs()
        .catch { emit(emptyList()) }

    override fun observePlaylistUrls(): Flow<List<String>> = playlistDao
        .observePlaylistUrls()
        .catch { emit(emptyList()) }

    override fun observe(url: String): Flow<Playlist?> = playlistDao
        .observeByUrl(url)
        .catch { emit(null) }

    override fun observePlaylistWithChannels(url: String): Flow<PlaylistWithChannels?> = playlistDao
        .observeByUrlWithChannels(url)
        .catch { emit(null) }

    override suspend fun getPlaylistWithChannels(url: String): PlaylistWithChannels? = playlistDao.getByUrlWithChannels(url)

    override suspend fun get(url: String): Playlist? = playlistDao.get(url)

    override suspend fun getAll(): List<Playlist> = playlistDao.getAll()

    override suspend fun getAllAutoRefresh(): List<Playlist> = playlistDao.getAllAutoRefresh()

    override suspend fun getBySource(source: DataSource): List<Playlist> = playlistDao.getBySource(source)

    override suspend fun getCategoriesByPlaylistUrlIgnoreHidden(
        url: String,
        query: String
    ): List<String> = playlistDao.get(url).let { playlist ->
        val pinnedCategories = playlist?.pinnedCategories ?: emptyList()
        val hiddenCategories = playlist?.hiddenCategories ?: emptyList()
        channelDao
            .getCategoriesByPlaylistUrl(url, query)
            .filterNot { it in hiddenCategories }
            .sortedByDescending { it in pinnedCategories }
    }

    override fun observeCategoriesByPlaylistUrlIgnoreHidden(
        url: String,
        query: String
    ): Flow<List<String>> = playlistDao.observeByUrl(url).flatMapLatest { playlist ->
        playlist ?: return@flatMapLatest flowOf()
        val pinnedCategories = playlist.pinnedCategories
        val hiddenCategories = playlist.hiddenCategories
        channelDao
            .observeCategoriesByPlaylistUrl(playlist.url, query)
            .map { categories ->
                categories
                    .filterNot { it in hiddenCategories }
                    .sortedByDescending { it in pinnedCategories }
            }
    }
        .flowOn(Dispatchers.Default)

    override suspend fun unsubscribe(url: String): Playlist? {
        val playlist = playlistDao.get(url)
        channelDao.deleteByPlaylistUrl(url)
        return playlist?.also {
            playlistDao.delete(it)
        }
    }

    override suspend fun onUpdatePlaylistTitle(url: String, title: String) = playlistDao.updateTitle(url, title)

    override suspend fun onUpdatePlaylistUserAgent(url: String, userAgent: String?) = playlistDao.updateUserAgent(url, userAgent)

    override fun observeAllCounts(): Flow<Map<Playlist, Int>> = playlistDao.observeAllCounts()
            .map { it.toMap() }
            .catch { emit(emptyMap()) }

    override suspend fun readEpisodesOrThrow(series: Channel): List<XtreamChannelInfo.Episode> {
        val playlist = checkNotNull(get(series.playlistUrl)) { "playlist is not exist" }
        val seriesInfo = xtreamParser.getSeriesInfoOrThrow(
            input = XtreamInput.decodeFromPlaylistUrl(playlist.url),
            seriesId = Url(series.url).rawSegments.last().toInt()
        )
        return seriesInfo.episodes.flatMap { it.value }
    }

    override suspend fun deleteEpgPlaylistAndProgrammes(epgUrl: String) {
        playlistDao.deleteByUrl(epgUrl)
        programmeDao.deleteAllByEpgUrl(epgUrl)
        playlistDao.removeEpgUrlForAllPlaylists(epgUrl)
    }

    override suspend fun onUpdateEpgPlaylist(useCase: PlaylistRepository.EpgPlaylistUseCase) {
        when (useCase) {
            is PlaylistRepository.EpgPlaylistUseCase.Check -> {
                playlistDao.updateEpgUrls(useCase.playlistUrl) { epgUrls ->
                    if (useCase.action) epgUrls + useCase.epgUrl
                    else epgUrls - useCase.epgUrl
                }
            }

            is PlaylistRepository.EpgPlaylistUseCase.Upward -> {
                val epgUrl = useCase.epgUrl
                playlistDao.updateEpgUrls(useCase.playlistUrl) { epgUrls ->
                    val index = epgUrls.indexOf(epgUrl)
                    if (index <= 0) epgUrls
                    else with(epgUrls) {
                        take(index - 1) + epgUrl + this[index - 1] + drop(index + 1)
                    }
                }
            }
        }
    }

    override suspend fun onUpdatePlaylistAutoRefreshProgrammes(playlistUrl: String) {
        val playlist = playlistDao.get(playlistUrl) ?: return
        playlistDao.updatePlaylistAutoRefreshProgrammes(
            playlistUrl,
            !playlist.autoRefreshProgrammes
        )
    }

    private val filenameWithTimezone: String get() = "File_${System.currentTimeMillis()}"

    private inline fun Reader.forEachLine(action: (String) -> Unit): Unit =
        useLines { it.forEach(action) }

    private fun String.isSupportedNetworkUrl(): Boolean = startsWithAny(
        "http://",
        "https://",
        ignoreCase = true
    )

    private fun String.isSupportedAndroidUrl(): Boolean = startsWithAny(
        ContentResolver.SCHEME_FILE,
        ContentResolver.SCHEME_CONTENT,
        ignoreCase = true
    )

    private suspend fun String.copyToInternalDirPath(): String {
        if (!isSupportedAndroidUrl()) return this
        val uri = this.toUri()
        if (uri.scheme == ContentResolver.SCHEME_FILE) return uri.toString()
        return withContext(Dispatchers.IO) {
            val contentResolver = context.contentResolver
            val filename = uri.readFileName(contentResolver) ?: filenameWithTimezone
            val destinationFile = File(context.filesDir, filename)

            val success = uri.copyToFile(contentResolver, destinationFile)
            if (!success) {
                return@withContext this@copyToInternalDirPath
            }

            val newUrl = Uri.decode(destinationFile.toUri().toString())
            playlistDao.updateUrl(this@copyToInternalDirPath, newUrl)
            newUrl
        }
    }

    private fun openNetworkInput(url: String): InputStream? {
        val request = Request.Builder()
            .url(url)
            .build()
        val response = okHttpClient.newCall(request).execute()
        return response.body?.byteStream()
    }

    private fun openAndroidInput(url: String): InputStream? {
        val uri = url.toUri()
        return context.contentResolver.openInputStream(uri)
    }
}
