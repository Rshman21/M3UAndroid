package com.m3u.data.worker

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.graphics.drawable.Icon
import androidx.hilt.work.HiltWorker
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ForegroundInfo
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.m3u.core.architecture.logger.Logger
import com.m3u.core.architecture.logger.Profiles
import com.m3u.core.architecture.logger.install
import com.m3u.data.R
import com.m3u.data.database.model.DataSource
import com.m3u.data.parser.xtream.XtreamInput
import com.m3u.data.repository.playlist.PlaylistRepository
import com.m3u.data.repository.programme.ProgrammeRepository
import com.m3u.i18n.R.string
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import java.util.concurrent.atomic.AtomicInteger

@HiltWorker
class SubscriptionWorker @AssistedInject constructor(
    @Assisted private val context: Context,
    @Assisted params: WorkerParameters,
    private val playlistRepository: PlaylistRepository,
    private val programmeRepository: ProgrammeRepository,
    private val notificationManager: NotificationManager,
    private val workManager: WorkManager,
    delegate: Logger
) : CoroutineWorker(context, params) {
    private val logger = delegate.install(Profiles.WORKER_SUBSCRIPTION)

    private val dataSource = inputData
        .getString(INPUT_STRING_DATA_SOURCE_VALUE)
        ?.let { DataSource.ofOrNull(it) }

    private val title = inputData.getString(INPUT_STRING_TITLE)
    private val basicUrl = inputData.getString(INPUT_STRING_BASIC_URL)
    private val username = inputData.getString(INPUT_STRING_USERNAME)
    private val password = inputData.getString(INPUT_STRING_PASSWORD)
    private val url = inputData.getString(INPUT_STRING_URL)
    private val epgPlaylistUrl = inputData.getString(INPUT_STRING_EPG_PLAYLIST_URL)
    private val epgIgnoreCache = inputData.getBoolean(INPUT_BOOLEAN_EPG_IGNORE_CACHE, false)
    private val notificationId: Int by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        ATOMIC_NOTIFICATION_ID.incrementAndGet()
    }

    override suspend fun doWork(): Result = coroutineScope {
        dataSource ?: return@coroutineScope Result.failure()
        createChannel()
        
        // 确保协程取消时通知也被取消
        coroutineContext[Job]?.invokeOnCompletion { cause ->
            if (cause is CancellationException) {
                notificationManager.cancel(notificationId)
            }
        }

        try {
            when (dataSource) {
                DataSource.M3U -> {
                    val title = title ?: return@coroutineScope Result.failure()
                    val url = url ?: return@coroutineScope Result.failure()
                    if (title.isEmpty()) {
                        notifyError(context.getString(string.data_error_empty_title))
                        Result.failure()
                    } else {
                        var total = 0
                        // 现在 Repository 会在内部校验，如果网络失败会直接抛异常，不会清空数据
                        playlistRepository.m3uOrThrow(title, url) { count ->
                            total = count
                            val notification = createN10nBuilder()
                                .setContentText(findChannelProgressContentText(count))
                                .setActions(cancelAction)
                                .setOngoing(true)
                                .build()
                            notificationManager.notify(notificationId, notification)
                        }

                        // 成功完成
                        createN10nBuilder()
                            .setContentText(findCompleteContentText(total))
                            .setOngoing(false)
                            .setAutoCancel(true)
                            .buildThenNotify()
                        Result.success()
                    }
                }

                DataSource.EPG -> {
                    val playlistUrl = epgPlaylistUrl ?: return@coroutineScope Result.failure()
                    val ignoreCache = epgIgnoreCache
                    programmeRepository.checkOrRefreshProgrammesOrThrow(
                        playlistUrl,
                        ignoreCache = ignoreCache
                    )
                        .onEach { count ->
                            val notification = createN10nBuilder()
                                .setContentText(findProgrammeProgressContentText(count))
                                .setActions(cancelAction)
                                .setOngoing(true)
                                .build()
                            notificationManager.notify(notificationId, notification)
                        }
                        .launchIn(this)
                    Result.success()
                }

                DataSource.Xtream -> {
                    title ?: return@coroutineScope Result.failure()
                    basicUrl ?: return@coroutineScope Result.failure()
                    username ?: return@coroutineScope Result.failure()
                    password ?: return@coroutineScope Result.failure()
                    if (title.isEmpty()) {
                        url ?: return@coroutineScope Result.failure()
                        notifyError(context.getString(string.data_error_empty_title))
                        Result.failure()
                    } else {
                        val type = url?.let { XtreamInput.decodeFromPlaylistUrlOrNull(it)?.type }
                        var total = 0
                        playlistRepository.xtreamOrThrow(
                            title, basicUrl, username, password, type
                        ) { count ->
                            total = count
                            val notification = createN10nBuilder()
                                .setContentText(findChannelProgressContentText(count))
                                .setActions(cancelAction)
                                .setOngoing(true)
                                .build()
                            notificationManager.notify(notificationId, notification)
                        }
                        createN10nBuilder()
                            .setContentText(findCompleteContentText(total))
                            .setOngoing(false)
                            .setAutoCancel(true)
                            .buildThenNotify()
                        Result.success()
                    }
                }

                else -> Result.failure()
            }
        } catch (e: Exception) {
            e.printStackTrace()
            // 捕获所有异常（网络、解析等），显示错误通知
            notifyError(e.localizedMessage.orEmpty())
            // 返回 Failure，不再重试，防止死循环
            Result.failure()
        }
    }

    // 辅助方法：发送错误通知
    private fun notifyError(message: String) {
        val notification = createN10nBuilder()
            .setContentTitle("Update Failed") // 或者使用资源文件 string.data_error_update_failed
            .setContentText(message)
            .setSmallIcon(R.drawable.round_error_outline_24) // 确保有一个错误图标
            .setColor(Color.RED)
            .setOngoing(false)
            .setAutoCancel(true)
            .build()
        notificationManager.notify(notificationId, notification)
    }

    private fun createChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID, NOTIFICATION_NAME, NotificationManager.IMPORTANCE_LOW
        )
        channel.description = "display subscribe task progress"
        notificationManager.createNotificationChannel(channel)
    }

    private fun Notification.Builder.buildThenNotify() {
        notificationManager.notify(notificationId, build())
    }

    override suspend fun getForegroundInfo(): ForegroundInfo {
        return ForegroundInfo(notificationId, createN10nBuilder().build())
    }

    private fun createN10nBuilder(): Notification.Builder =
        Notification.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.round_file_download_24)
            .setContentTitle(
                when (dataSource) {
                    DataSource.EPG -> epgPlaylistUrl
                    else -> title
                }
            )
            .setOngoing(true)

    private fun findCancelActionTitle() =
        context.getString(string.data_worker_subscription_action_cancel)

    private fun findRetryActionTitle() =
        context.getString(string.data_worker_subscription_action_retry)

    private fun findCompleteContentText(total: Int) =
        context.getString(string.data_worker_subscription_content_completed, total)

    private fun findChannelProgressContentText(count: Int) =
        context.getString(string.data_worker_subscription_content_channel_progress, count)

    private fun findProgrammeProgressContentText(count: Int) =
        context.getString(string.data_worker_subscription_content_programme_progress, count)

    private val cancelAction: Notification.Action by lazy {
        Notification.Action.Builder(
            Icon.createWithResource(
                context,
                R.drawable.round_cancel_24
            ),
            findCancelActionTitle(),
            workManager.createCancelPendingIntent(id)
        )
            .build()
    }
    
    // Retry action 被我移除了，因为你不想在失败时让用户点重试然后又无限失败
    // 如果需要保留，可以在 notifyError 里加回去

    companion object {
        private const val CHANNEL_ID = "subscribe_channel"
        private const val NOTIFICATION_NAME = "subscribe task"
        // ... (其他常量保持不变)
        private const val INPUT_STRING_TITLE = "title"
        private const val INPUT_STRING_URL = "url"
        private const val INPUT_STRING_EPG_PLAYLIST_URL = "epg"
        private const val INPUT_BOOLEAN_EPG_IGNORE_CACHE = "ignore_cache"
        private const val INPUT_STRING_BASIC_URL = "basic_url"
        private const val INPUT_STRING_USERNAME = "username"
        private const val INPUT_STRING_PASSWORD = "password"
        private const val INPUT_STRING_DATA_SOURCE_VALUE = "data-source"
        const val TAG = "subscription"
        
        // ... (m3u, epg, xtream 的 companion object 方法保持不变)
        // 仅需确保 import 正确，代码逻辑不需要变
        
        fun m3u(workManager: WorkManager, title: String, url: String) {
             workManager.cancelAllWorkByTag(url)
             val request = OneTimeWorkRequestBuilder<SubscriptionWorker>()
                 .setInputData(workDataOf(INPUT_STRING_TITLE to title, INPUT_STRING_URL to url, INPUT_STRING_DATA_SOURCE_VALUE to DataSource.M3U.value))
                 .addTag(url).addTag(TAG).addTag(DataSource.M3U.value)
                 .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                 .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                 .build()
             workManager.enqueue(request)
        }
        
        fun epg(workManager: WorkManager, playlistUrl: String, ignoreCache: Boolean) {
            workManager.cancelAllWorkByTag(playlistUrl)
            val request = OneTimeWorkRequestBuilder<SubscriptionWorker>()
                .setInputData(workDataOf(INPUT_STRING_EPG_PLAYLIST_URL to playlistUrl, INPUT_BOOLEAN_EPG_IGNORE_CACHE to ignoreCache, INPUT_STRING_DATA_SOURCE_VALUE to DataSource.EPG.value))
                .addTag(playlistUrl).addTag(TAG).addTag(DataSource.EPG.value)
                .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                .build()
            workManager.enqueue(request)
        }
        
        fun xtream(workManager: WorkManager, title: String, url: String, basicUrl: String, username: String, password: String) {
            workManager.cancelAllWorkByTag(url)
            workManager.cancelAllWorkByTag(basicUrl)
            val request = OneTimeWorkRequestBuilder<SubscriptionWorker>()
                .setInputData(workDataOf(INPUT_STRING_TITLE to title, INPUT_STRING_URL to url, INPUT_STRING_BASIC_URL to basicUrl, INPUT_STRING_USERNAME to username, INPUT_STRING_PASSWORD to password, INPUT_STRING_DATA_SOURCE_VALUE to DataSource.Xtream.value))
                .addTag(url).addTag(basicUrl).addTag(DataSource.Xtream.value).addTag(TAG)
                .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                .build()
            workManager.enqueue(request)
        }

        private val ATOMIC_NOTIFICATION_ID = AtomicInteger()
    }
}