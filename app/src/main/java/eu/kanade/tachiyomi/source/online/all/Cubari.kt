package eu.kanade.tachiyomi.source.online.all

import android.net.Uri
import eu.kanade.tachiyomi.data.database.models.Chapter
import eu.kanade.tachiyomi.data.database.models.toChapter
import eu.kanade.tachiyomi.data.preference.PreferencesHelper
import eu.kanade.tachiyomi.domain.manga.models.Manga
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.online.DelegatedHttpSource
import java.util.Locale
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import yokai.i18n.MR
import yokai.util.lang.getString

class Cubari : DelegatedHttpSource() {
    override val domainName: String = "cubari"
    override fun canOpenUrl(uri: Uri): Boolean = true

    override fun chapterUrl(uri: Uri): String? = null

    override fun pageNumber(uri: Uri): Int? = uri.pathSegments.getOrNull(4)?.toIntOrNull()

    override suspend fun fetchMangaFromChapterUrl(uri: Uri): Triple<Chapter, Manga, List<SChapter>>? {
        val cubariType = uri.pathSegments.getOrNull(1)?.lowercase(Locale.ROOT) ?: return null
        val cubariPath = uri.pathSegments.getOrNull(2) ?: return null
        val chapterNumber = uri.pathSegments.getOrNull(3)?.replace("-", ".")?.toFloatOrNull() ?: return null
        val mangaUrl = "/read/$cubariType/$cubariPath"
        return withContext(Dispatchers.IO) {
            val deferredManga = async {
                getManga.awaitByUrlAndSource(mangaUrl, delegate?.id!!) ?: getMangaInfo(mangaUrl)
            }
            val deferredChapters = async {
                getManga.awaitByUrlAndSource(mangaUrl, delegate?.id!!)?.let { manga ->
                    val chapters = getChapter.awaitAll(manga, false)
                    val chapter = findChapter(chapters, cubariType, chapterNumber)
                    if (chapter != null) {
                        return@async chapters
                    }
                }
                getChapters(mangaUrl)
            }
            val manga = deferredManga.await()
            val chapters = deferredChapters.await()
            val context = Injekt.get<PreferencesHelper>().context
            val trueChapter = findChapter(chapters, cubariType, chapterNumber)?.toChapter()
                ?: error(
                    context.getString(MR.strings.chapter_not_found),
                )
            if (manga != null) {
                Triple(trueChapter, manga, chapters.orEmpty())
            } else {
                null
            }
        }
    }

    fun findChapter(chapters: List<SChapter>?, cubariType: String, chapterNumber: Float): SChapter? {
        return when (cubariType) {
            "imgur" -> chapters?.firstOrNull()
            else -> chapters?.find { it.chapter_number == chapterNumber }
        }
    }
}
