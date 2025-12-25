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

        // Regex for parsing #EXTINF line: duration, attributes, title
        private val infoRegex = """(-?\d+)(.*),(.+)""".toRegex()
        // Regex for parsing key=value properties in #EXTINF attributes
        private val metadataRegex = """([\w-_.]+)=\s*(?:"([^"]*)"|(\S+))""".toRegex()
        // Regex for parsing #KODIPROP lines
        private val kodiPropRegex = """([^=]+)=(.+)""".toRegex()

        private const val M3U_TVG_LOGO_MARK = "tvg-logo"
        const val M3U_TVG_ID_MARK = "tvg-id"
        const val M3U_TVG_NAME_MARK = "tvg-name"
        const val M3U_GROUP_TITLE_MARK = "group-title"

        const val KODI_LICENSE_TYPE = "inputstream.adaptive.license_type"
        const val KODI_LICENSE_KEY = "inputstream.adaptive.license_key"
    }

    override fun parse(input: InputStream): Flow<M3UData> = flow {
        val reader = input.bufferedReader()
        
        // 缓存当前频道的元数据
        var infoMatch: MatchResult? = null
        val kodiMatches = mutableListOf<String>()

        reader.forEachLine { line ->
            val currentLine = line.trim()
            if (currentLine.isEmpty()) return@forEachLine

            when {
                // 1. 处理文件头，忽略
                currentLine.startsWith(M3U_HEADER_MARK) -> {
                    return@forEachLine
                }

                // 2. 处理 #EXTINF 行 (新频道的开始)
                currentLine.startsWith(M3U_INFO_MARK) -> {
                    infoMatch = infoRegex.matchEntire(currentLine.substringAfter(M3U_INFO_MARK))
                    // 新频道开始，清空之前的 KODI 属性（以防万一）
                    kodiMatches.clear()
                }

                // 3. 处理 KODI 属性行
                currentLine.startsWith(KODI_MARK) -> {
                    kodiMatches.add(currentLine.substringAfter(KODI_MARK).trim())
                }

                // 4. 处理以 # 开头的其他行 (需求1：被注释的链接)
                currentLine.startsWith("#") -> {
                    // 如果代码走到这里，说明是 # 开头，但不是 EXTINF 也不是 KODIPROP
                    // 这通常意味着这是一个被注释掉的 URL (如 #https://...) 或者其他无关注释
                    // 此时应该视为当前频道无效，清空缓存，跳过生成。
                    infoMatch = null
                    kodiMatches.clear()
                }

                // 5. 处理 URL 行 (需求2：取第一个链接)
                else -> {
                    // 只有当 infoMatch 不为空时，才说明这是一个合法的频道 URL
                    // 如果 infoMatch 为空，说明：
                    //   a. 这是文件开头的孤立 URL
                    //   b. 这是"多链接"情况下的第二个及之后的 URL（因为第一个 URL 处理完后会清空 infoMatch）
                    //   c. 之前的 EXTINF 被上面的逻辑(4)废弃了
                    if (infoMatch != null) {
                        val duration = infoMatch?.groups?.get(1)?.value?.toDoubleOrNull() ?: -1.0
                        val attributesText = infoMatch?.groups?.get(2)?.value.orEmpty()
                        val title = infoMatch?.groups?.get(3)?.value.orEmpty().trim()

                        // 解析 EXTINF 中的属性 (tvg-id, group-title 等)
                        val metadata = buildMap {
                            val matches = metadataRegex.findAll(attributesText)
                            for (match in matches) {
                                val key = match.groups[1]!!.value
                                // 取 group 2 (带引号的值) 或 group 3 (不带引号的值)
                                val value = (match.groups[2]?.value ?: match.groups[3]?.value)?.trim()
                                if (!value.isNullOrBlank()) {
                                    put(key.trim(), value)
                                }
                            }
                        }

                        // 解析 KODI 属性
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
                            url = currentLine, // 这里的 currentLine 就是 URL
                            duration = duration,
                            licenseType = kodiMetadata[KODI_LICENSE_TYPE],
                            licenseKey = kodiMetadata[KODI_LICENSE_KEY]
                        )

                        emit(entry)

                        // 【关键】发射完第一个链接的数据后，立即清空 infoMatch。
                        // 这样如果下一行还是一个 URL (多链接情况)，infoMatch 就是 null，
                        // 会直接跳过 if (infoMatch != null) 块，从而实现"只取第一个链接"。
                        infoMatch = null
                        kodiMatches.clear()
                    }
                }
            }
        }
    }
        .flowOn(Dispatchers.Default)
}
