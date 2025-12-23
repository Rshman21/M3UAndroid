package com.m3u.data.repository.playlist

import android.content.ContentResolver
import android.content.Context
import android.net.Uri
import androidx.core.net.toUri
import androidx.work.WorkManager
import com.m3u.core.architecture.logger.Logger
import com.m3u.core.architecture.logger.Profiles
import com.m3u.core.architecture.logger.execute
import com.m3u.core.architecture.logger.install
import com.m3u.core.architecture.logger.post
import com.m3u.core.architecture.logger.sandBox
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
import com.m3u.data.database.model.PlaylistWithCount
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
private const val BUFFER_RESTORE_CAPACITY = 400

internal class PlaylistRepositoryImpl @Inject constructor(
    private val playlistDao: PlaylistDao,
    private val channelDao: ChannelDao,
    private val programmeDao: ProgrammeDao,
    delegate: Logger,
    @OkhttpClient(true) private val okHttpClient: OkHttpClient,
    private val m3uParser: M3UParser,
    private val xtreamParser: XtreamParser,
    private val workManager: WorkManager,
    @ApplicationContext private val context: Context,
    private val settings: Settings
) : PlaylistRepository {
    private val logger = delegate.install(Profiles.REPOS_PLAYLIST)

    override suspend fun m3uOrThrow(
        title: String,
        url: String,
        callback: (count: Int) -> Unit
    ) {
        val actualUrl = url.actualUrl()
        // 1. 预检查：尝试连接或准备文件
        val inputStream = when {
            url.isSupportedNetworkUrl() -> openNetworkInput(actualUrl)
            url.isSupportedAndroidUrl() -> openAndroidInput(actualUrl)
            else -> throw IllegalArgumentException("Unsupported URL scheme: $actualUrl")
        } ?: throw IllegalArgumentException("Cannot open input stream for: $actualUrl")

        // 2. 解析：将数据先解析到内存列表中
        val newChannels = mutableListOf<Channel>()
        try {
            inputStream.use { input ->
                m3uParser.parse(input.buffered())
                    .collect { m3uData ->
                        newChannels.add(m3uData.toChannel(actualUrl))
                    }
            }
        } catch (e: Exception) {
            logger.log("Parse failed, aborting update: ${e.message}")
            throw e
        }

        if (newChannels.isEmpty()) {
            throw RuntimeException("Stream list is empty, aborting update.")
        }

        // 3. 写入：数据准备完毕，现在开始安全的数据库事务操作
        val playlistStrategy = settings[PreferencesKeys.PLAYLIST_STRATEGY]
        
        val playlist = playlistDao.get(actualUrl)?.copy(
            title = title,
            source = DataSource.M3U
        ) ?: Playlist(title, actualUrl, source = DataSource.M3U)
        playlistDao.insertOrReplace(playlist)

        val favOrHiddenRelationIds = when (playlistStrategy) {
            PlaylistStrategy.ALL -> emptyList()
            else -> channelDao.getFavOrHiddenRelationIdsByPlaylistUrl(url)
        }
        val favOrHiddenUrls = when (playlistStrategy) {
            PlaylistStrategy.ALL -> emptyList()
            else -> channelDao.getFavOrHiddenUrlsByPlaylistUrlNotContainsRelationId(url)
        }

        when (playlistStrategy) {
            PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(url)
            PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(url)
        }

        val finalChannels = newChannels.filterNot { channel ->
            val relationId = channel.relationId
            when {
                relationId == null || relationId.isBlank() -> channel.url in favOrHiddenUrls
                else -> relationId in favOrHiddenRelationIds
            }
        }

        finalChannels.chunked(BUFFER_M3U_CAPACITY).forEach { batch ->
            channelDao.insertOrReplaceAll(*batch.toTypedArray())
            callback(finalChannels.size)
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

        // 【关键修改】这里使用具名参数 source = DataSource.Xtream，解决了编译报错
        val livePlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_LIVE),
            serverProtocol = serverProtocol, port = port
        ).let { url -> 
            playlistDao.get(url)?.takeIf { it.source == DataSource.Xtream }?.copy(title = title) 
            ?: Playlist(title = title, url = url, source = DataSource.Xtream) 
        }

        val vodPlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_VOD),
            serverProtocol = serverProtocol, port = port
        ).let { url -> 
            playlistDao.get(url)?.takeIf { it.source == DataSource.Xtream }?.copy(title = title) 
            ?: Playlist(title = title, url = url, source = DataSource.Xtream) 
        }

        val seriesPlaylist = XtreamInput.encodeToPlaylistUrl(
            input = input.copy(type = DataSource.Xtream.TYPE_SERIES),
            serverProtocol = serverProtocol, port = port
        ).let { url -> 
            playlistDao.get(url)?.takeIf { it.source == DataSource.Xtream }?.copy(title = title) 
            ?: Playlist(title = title, url = url, source = DataSource.Xtream) 
        }

        val newChannels = mutableListOf<Channel>()
        try {
            xtreamParser.parse(input).collect { current ->
                val channel = when (current) {
                    is XtreamLive -> current.toChannel(basicUrl, username, password, livePlaylist.url, liveCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty(), liveContainerExtension)
                    is XtreamVod -> current.toChannel(basicUrl, username, password, vodPlaylist.url, vodCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty())
                    is XtreamSerial -> current.asChannel(basicUrl, username, password, seriesPlaylist.url, serialCategories.find { it.categoryId == current.categoryId }?.categoryName.orEmpty())
                }
                newChannels.add(channel)
            }
        } catch (e: Exception) {
            logger.log("Xtream parse failed: ${e.message}")
            throw e
        }

        if (newChannels.isEmpty()) {
             throw RuntimeException("Xtream source is empty, aborting.")
        }

        val requiredLives = type == null || type == DataSource.Xtream.TYPE_LIVE
        val requiredVods = type == null || type == DataSource.Xtream.TYPE_VOD
        val requiredSeries = type == null || type == DataSource.Xtream.TYPE_SERIES
        val playlistStrategy = settings[PreferencesKeys.PLAYLIST_STRATEGY]

        if (requiredLives) updatePlaylistAndClearOld(livePlaylist, playlistStrategy)
        if (requiredVods) updatePlaylistAndClearOld(vodPlaylist, playlistStrategy)
        if (requiredSeries) updatePlaylistAndClearOld(seriesPlaylist, playlistStrategy)

        val favOrHiddenRelationIds = channelDao.getFavOrHiddenRelationIdsByPlaylistUrl(
            livePlaylist.url, vodPlaylist.url, seriesPlaylist.url
        )
        
        val finalChannels = newChannels.filterNot { channel ->
            val relationId = channel.relationId
            relationId != null && relationId in favOrHiddenRelationIds
        }

        finalChannels.chunked(BUFFER_M3U_CAPACITY).forEach { batch ->
            channelDao.insertOrReplaceAll(*batch.toTypedArray())
            callback(finalChannels.size)
        }
    }
    
    private suspend fun updatePlaylistAndClearOld(playlist: Playlist, strategy: Int) {
        playlistDao.insertOrReplace(playlist)
        when (strategy) {
            PlaylistStrategy.ALL -> channelDao.deleteByPlaylistUrl(playlist.url)
            PlaylistStrategy.KEEP -> channelDao.deleteByPlaylistUrlIgnoreFavOrHidden(playlist.url)
        }
    }

    override suspend fun insertEpgAsPlaylist(title: String, epg: String) {
        playlistDao.insertOrReplace(
            Playlist(title = title, url = epg, source = DataSource.EPG)
        )
    }

    override suspend fun refresh(url: String) = logger.sandBox {
        val playlist = checkNotNull(get(url)) { "Cannot find playlist: $url" }
        check(!playlist.fromLocal) { "refreshing is not needed for local storage playlist." }

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
                if (playlist.fromLocal) return@forEach
                val encodedPlaylist = json.encodeToString(playlist)
                writer.appendLine(BackupOrRestoreContracts.wrapPlaylist(encodedPlaylist))
                channels.forEach { channel ->
                    val encodedChannel = json.encodeToString(channel)
                    writer.appendLine(BackupOrRestoreContracts.wrapChannel(encodedChannel))
                }
            }
            writer.flush()
        }
    }

    override suspend fun restoreOrThrow(uri: Uri) = logger.sandBox {
        withContext(Dispatchers.IO) {
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
                    }
                }
                mutex.withLock { channelDao.insertOrReplaceAll(*channels.toTypedArray()) }
            }
        }
    }

    override suspend fun pinOrUnpinCategory(url: String, category: String) { playlistDao.updatePinnedCategories(url) { if (category in it) it - category else it + category } }
    override suspend fun hideOrUnhideCategory(url: String, category: String) { playlistDao.hideOrUnhideCategory(url, category) }
    override fun observeAll(): Flow<List<Playlist>> = playlistDao.observeAll().catch { emit(emptyList()) }
    override fun observeAllEpgs(): Flow<List<Playlist>> = playlistDao.observeAllEpgs().catch { emit(emptyList()) }
    override fun observePlaylistUrls(): Flow<List<String>> = playlistDao.observePlaylistUrls().catch { emit(emptyList()) }
    override fun observe(url: String): Flow<Playlist?> = playlistDao.observeByUrl(url).catch { emit(null) }
    override fun observePlaylistWithChannels(url: String): Flow<PlaylistWithChannels?> = playlistDao.observeByUrlWithChannels(url).catch { emit(null) }
    override suspend fun getPlaylistWithChannels(url: String): PlaylistWithChannels? = playlistDao.getByUrlWithChannels(url)
    override suspend fun get(url: String): Playlist? = playlistDao.get(url)
    override suspend fun getAll(): List<Playlist> = playlistDao.getAll()
    override suspend fun getAllAutoRefresh(): List<Playlist> = playlistDao.getAllAutoRefresh()
    override suspend fun getBySource(source: DataSource): List<Playlist> = playlistDao.getBySource(source)
    override suspend fun getCategoriesByPlaylistUrlIgnoreHidden(url: String, query: String): List<String> = playlistDao.get(url)?.let { playlist ->
        channelDao.getCategoriesByPlaylistUrl(url, query).filterNot { it in playlist.hiddenCategories }.sortedByDescending { it in playlist.pinnedCategories }
    } ?: emptyList()
    override fun observeCategoriesByPlaylistUrlIgnoreHidden(url: String, query: String): Flow<List<String>> = playlistDao.observeByUrl(url).flatMapLatest { playlist ->
        playlist ?: return@flatMapLatest flowOf()
        channelDao.observeCategoriesByPlaylistUrl(playlist.url, query).map { categories -> categories.filterNot { it in playlist.hiddenCategories }.sortedByDescending { it in playlist.pinnedCategories } }
    }.flowOn(Dispatchers.Default)
    override suspend fun unsubscribe(url: String): Playlist? {
        val playlist = playlistDao.get(url)
        channelDao.deleteByPlaylistUrl(url)
        playlist?.also { playlistDao.delete(it) }
        return playlist
    }
    override suspend fun onUpdatePlaylistTitle(url: String, title: String) { playlistDao.updateTitle(url, title) }
    override suspend fun onUpdatePlaylistUserAgent(url: String, userAgent: String?) { playlistDao.updateUserAgent(url, userAgent) }
    override fun observeAllCounts(): Flow<Map<Playlist, Int>> = playlistDao.observeAllCounts().map { it.toMap() }.catch { emit(emptyMap()) }
    override suspend fun readEpisodesOrThrow(series: Channel): List<XtreamChannelInfo.Episode> {
        val playlist = checkNotNull(get(series.playlistUrl))
        val seriesInfo = xtreamParser.getSeriesInfoOrThrow(XtreamInput.decodeFromPlaylistUrl(playlist.url), Url(series.url).rawSegments.last().toInt())
        return seriesInfo.episodes.flatMap { it.value }
    }
    override suspend fun deleteEpgPlaylistAndProgrammes(epgUrl: String) {
        playlistDao.deleteByUrl(epgUrl)
        programmeDao.deleteAllByEpgUrl(epgUrl)
        playlistDao.removeEpgUrlForAllPlaylists(epgUrl)
    }
    override suspend fun onUpdateEpgPlaylist(useCase: PlaylistRepository.EpgPlaylistUseCase) {
        when (useCase) {
            is PlaylistRepository.EpgPlaylistUseCase.Check -> playlistDao.updateEpgUrls(useCase.playlistUrl) { if (useCase.action) it + useCase.epgUrl else it - useCase.epgUrl }
            is PlaylistRepository.EpgPlaylistUseCase.Upward -> playlistDao.updateEpgUrls(useCase.playlistUrl) { with(it) { val index = indexOf(useCase.epgUrl); if (index <= 0) it else take(index - 1) + useCase.epgUrl + this[index - 1] + drop(index + 1) } }
        }
    }
    override suspend fun onUpdatePlaylistAutoRefreshProgrammes(playlistUrl: String) {
        val playlist = playlistDao.get(playlistUrl) ?: return
        playlistDao.updatePlaylistAutoRefreshProgrammes(playlistUrl, !playlist.autoRefreshProgrammes)
    }
    
    private val filenameWithTimezone: String get() = "File_${System.currentTimeMillis()}"
    private inline fun Reader.forEachLine(action: (String) -> Unit): Unit = useLines { it.forEach(action) }
    private fun String.isSupportedNetworkUrl(): Boolean = startsWithAny("http://", "https://", ignoreCase = true)
    private fun String.isSupportedAndroidUrl(): Boolean = startsWithAny(ContentResolver.SCHEME_FILE, ContentResolver.SCHEME_CONTENT, ignoreCase = true)
    private suspend fun String.actualUrl(): String {
        if (!isSupportedAndroidUrl()) return this
        val uri = this.toUri()
        if (uri.scheme == ContentResolver.SCHEME_FILE) return uri.toString()
        return withContext(Dispatchers.IO) {
            val contentResolver = context.contentResolver
            val filename = uri.readFileName(contentResolver) ?: filenameWithTimezone
            val destinationFile = File(context.filesDir, filename)
            if (!uri.copyToFile(contentResolver, destinationFile)) return@withContext this@actualUrl
            val newUrl = Uri.decode(destinationFile.toUri().toString())
            playlistDao.updateUrl(this@actualUrl, newUrl)
            newUrl
        }
    }
    private fun openNetworkInput(url: String): InputStream? {
        val request = Request.Builder().url(url).build()
        val response = okHttpClient.newCall(request).execute()
        return if (response.isSuccessful) response.body?.byteStream() else null
    }
    private fun openAndroidInput(url: String): InputStream? = context.contentResolver.openInputStream(url.toUri())
}