package com.m3u.data.parser.m3u

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import java.io.InputStream
import javax.inject.Inject

internal class M3UParserImpl @Inject constructor() : M3UParser {
    companion object {
        private const val M3U_HEADER_MARK = "#EXTM3U"
        private const val M3U_INFO_MARK = "#EXTINF:"
        private const val KODI_MARK = "#KODIPROP:"

        private val infoRegex = """(-?\d+)(.*),(.+)""".toRegex()
        private val metadataRegex = """([\w-_.]+)=\s*(?:"([^"]*)"|(\S+))""".toRegex()
        private val kodiPropRegex = """([^=]+)=(.+)""".toRegex()

        private const val M3U_TVG_LOGO_MARK = "tvg-logo"
        const val M3U_TVG_ID_MARK = "tvg-id"
        const val M3U_TVG_NAME_MARK = "tvg-name"
        const val M3U_GROUP_TITLE_MARK = "group-title"

        const val KODI_LICENSE_TYPE = "inputstream.adaptive.license_type"
        const val KODI_LICENSE_KEY = "inputstream.adaptive.license_key"
    }

    override fun parse(input: InputStream): Flow<M3UData> = flow {
        // 使用 use 自动关闭流
        input.bufferedReader().use { reader ->
            var infoMatch: MatchResult? = null
            val kodiMatches = mutableListOf<String>()

            while (true) {
                // 在协程中手动读取行，避免 forEachLine 的作用域限制问题
                val line = try {
                    reader.readLine()
                } catch (e: Exception) {
                    null
                } ?: break // 读到末尾或出错退出循环

                val currentLine = line.trim()
                if (currentLine.isEmpty()) continue

                when {
                    // 1. 忽略文件头
                    currentLine.startsWith(M3U_HEADER_MARK) -> {
                        continue
                    }

                    // 2. 新频道信息开始
                    currentLine.startsWith(M3U_INFO_MARK) -> {
                        infoMatch = infoRegex.matchEntire(currentLine.substringAfter(M3U_INFO_MARK))
                        kodiMatches.clear()
                    }

                    // 3. KODI 属性
                    currentLine.startsWith(KODI_MARK) -> {
                        kodiMatches.add(currentLine.substringAfter(KODI_MARK).trim())
                    }

                    // 4. 其他以 # 开头的行 (视为注释/被屏蔽的链接)
                    currentLine.startsWith("#") -> {
                        // 丢弃之前的缓存，跳过当前条目
                        infoMatch = null
                        kodiMatches.clear()
                    }

                    // 5. 播放链接
                    else -> {
                        if (infoMatch != null) {
                            val duration = infoMatch?.groups?.get(1)?.value?.toDoubleOrNull() ?: -1.0
                            val attributesText = infoMatch?.groups?.get(2)?.value.orEmpty()
                            val title = infoMatch?.groups?.get(3)?.value.orEmpty().trim()

                            val metadata = buildMap {
                                val matches = metadataRegex.findAll(attributesText)
                                for (match in matches) {
                                    val key = match.groups[1]!!.value
                                    val value = (match.groups[2]?.value ?: match.groups[3]?.value)?.trim()
                                    if (!value.isNullOrBlank()) {
                                        put(key.trim(), value)
                                    }
                                }
                            }

                            val kodiMetadata = buildMap {
                                for (propLine in kodiMatches) {
                                    val match = kodiPropRegex.matchEntire(propLine) ?: continue
                                    val key = match.groups[1]?.value?.trim() ?: continue
                                    val value = match.groups[2]?.value?.trim() ?: continue
                                    put(key, value)
                                }
                            }

                            val entry = M3UData(
                                id = metadata[M3U_TVG_ID_MARK].orEmpty(),
                                name = metadata[M3U_TVG_NAME_MARK].orEmpty(),
                                cover = metadata[M3U_TVG_LOGO_MARK].orEmpty(),
                                group = metadata[M3U_GROUP_TITLE_MARK].orEmpty(),
                                title = title,
                                url = currentLine,
                                duration = duration,
                                licenseType = kodiMetadata[KODI_LICENSE_TYPE],
                                licenseKey = kodiMetadata[KODI_LICENSE_KEY]
                            )

                            // 这里的 emit 现在是安全的，因为它在 while 循环中，直接处于 flow 的挂起上下文中
                            emit(entry)

                            // 发送后立即清空，确保多链接情况下只取第一个
                            infoMatch = null
                            kodiMatches.clear()
                        }
                    }
                }
            }
        }
    }
    .flowOn(Dispatchers.IO)
}
