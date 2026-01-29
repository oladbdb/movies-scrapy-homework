import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import re

def clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    s = re.sub(r"\[\d+\]", "", s).strip()
    return s

def uniq_preserve(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def clean_join(parts):
    parts = [p.strip() for p in parts if p and p.strip()]
    s = " ".join(parts)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_year(text):
    if not text:
        return ""
    m = re.search(r"\b(18|19|20)\d{2}\b", text)
    return m.group(0) if m else ""

def normalize_title(title):
    if not title:
        return ""
    title = title.strip()
    title = re.sub(r"\s*\(([^)]*\bфильм\b[^)]*|\b(18|19|20)\d{2}\b[^)]*)\)\s*$", "", title, flags=re.IGNORECASE).strip()
    return title


class MoviesSpider(CrawlSpider):
    name = 'movies'
    allowed_domains = ['ru.wikipedia.org', 'imdb.com', 'www.imdb.com']
    start_urls = [
        'https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту'
    ]

    rules = (
        Rule(
            LinkExtractor(
                allow=r"/wiki/Категория:Фильмы_.*",
            ),
            follow=True,
        ),

        Rule(
            LinkExtractor(
                allow=r"/w/index\.php.*&pagefrom=",
                restrict_xpaths='//a[contains(text(), "Следующая страница")]',
            ),
            follow=True,
        ),

        Rule(
            LinkExtractor(
                allow=r"/wiki/[^:#]+$",
                deny=(
                    r"/wiki/Категория:.*", 
                    r"/wiki/Служебная:.*", 
                    r"/wiki/Файл:.*",
                    r"/wiki/Википедия:.*",
                    r"/wiki/Обсуждение:.*",
                ),
            ),
            callback="parse_item",
            follow=False,
        ),
    )

    def infobox_value(self, response, *labels):
        infobox = response.xpath('//table[contains(@class, "infobox")]')[0:1]
        if not infobox:
            return ""

        cond = " or ".join([f'contains(normalize-space(.), "{lab}")' for lab in labels])
        th = infobox.xpath(f'.//tr/th[{cond}]')[0:1]
        if not th:
            return ""

        td = th.xpath("./following-sibling::td[1]")[0:1]
        if not td:
            return ""

        link_texts = td.xpath('.//a[not(contains(@class,"image"))]/text()').getall()
        link_texts = [clean_text(x) for x in link_texts]

        raw_texts = td.xpath(
            './/text()[not(ancestor::sup) and not(ancestor::style) and not(ancestor::script)]'
        ).getall()
        raw_texts = [clean_text(x) for x in raw_texts]

        parts = uniq_preserve([x for x in (link_texts + raw_texts) if x])
        junk = {"—", "-", ":", "и"}
        parts = [p for p in parts if p not in junk]

        return "; ".join(parts).strip(" ;")

    def parse_item(self, response):
        infobox = response.xpath('//table[contains(@class, "infobox")]')[0:1]
        if not infobox:
            return

        title = response.xpath('//h1[@id="firstHeading"]/span[@class="mw-page-title-main"]/text()').get(default='').strip()
        title = normalize_title(title)

        genre = self.infobox_value(response, "Жанр", "Жанры")
        director = self.infobox_value(response, "Режиссёр", "Режиссеры", "Режиссёры")
        country = self.infobox_value(response, "Страна", "Страны")
        year_raw = self.infobox_value(response, "Год")
        year = extract_year(year_raw) or extract_year(response.text)

        imdb_link = self.extract_imdb_link(response)

        item = {
            "title": title,
            "genre": genre,
            "director": director,
            "country": country,
            "year": year,
            "imdb_rating": "",
        }

        if imdb_link:
            yield scrapy.Request(
                imdb_link,
                callback=self.parse_imdb,
                meta={"item": item},
                dont_filter=True,
                priority=1000,
            )
        else:
            yield item

    def extract_imdb_link(self, response):
        link = response.xpath(
            '//table[contains(@class, "infobox")]'
            '//tr['
            './/th//a[normalize-space()="IMDb"] or '
            './/th[normalize-space()="IMDb"]'
            ']'
            '//td//a[contains(@href, "imdb.com/title/")]/@href'
        ).get()
        if not link:
            link = response.xpath(
                '//a[contains(@href, "imdb.com/title/")]/@href'
            ).get()
        if not link:
            return ""
        link = link.strip()
        if link.startswith("//"):
            link = "https:" + link
        elif link.startswith("/"):
            link = "https://www.imdb.com" + link
        return link

    def parse_imdb(self, response):
        item = response.meta.get("item")
        if not item:
            return

        rating_xpath = (
            "//div[contains(text(), 'IMDb RATING')]/following-sibling::a"
            "//div[contains(@data-testid, 'rating')]/span/text()"
        )
        rating = response.xpath(rating_xpath).get()
        if rating:
            try:
                item["imdb_rating"] = str(float(rating.replace(',', '.')))
            except ValueError:
                item["imdb_rating"] = rating.strip()
        else:
            item["imdb_rating"] = ""

        yield item