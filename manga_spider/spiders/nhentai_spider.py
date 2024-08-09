from typing import Any, cast
import re
import json
from manga_spider.items import NHentaiMangaSpiderItem
from manga_spider.utils import completed_ids

import scrapy
from scrapy.http import Response


def get_num_favorites(response: Response) -> int:
    favorite_text = response.css(
        "#info > div.buttons > a.btn > span > span::text"
    ).get()
    favorite_text = cast(str | None, favorite_text)
    if favorite_text is not None:
        favorite_text = favorite_text.strip()
        if favorite_text != "":
            favorite_text = favorite_text.removeprefix("(")
            favorite_text = favorite_text.removesuffix(")")
            favorite_text = favorite_text.strip()
            if favorite_text != "":
                return int(favorite_text)
    return 0

def extract_id_from_link(link: str) -> int:
    m = re.search(r"/g/(\d+)", link)
    if m is not None:
        return int(m.group(1))
    else:
        raise RuntimeError(
            f"Unable parse the manga id from link: {link}"
        )

class NHentaiSpider(scrapy.Spider):
    name = "nhentai"
    start_urls = ["https://nhentai.net/"]

    def collect_manga_urls(self, latest_id: int) -> list[str]:
        ids = [id + 1 for id in range(latest_id)]
        exclude_ids = completed_ids(self.name)
        return [f"https://nhentai.net/g/{id}/" for id in ids if id not in exclude_ids]

    def parse(self, response: Response, **kwargs: Any) -> Any:
        latest_link = response.css(
            "#content .index-container:not(.index-popular) div.gallery a"
        ).attrib["href"]
        latest_id = extract_id_from_link(latest_link)
        for url in self.collect_manga_urls(latest_id=latest_id):
            yield scrapy.Request(url=url, callback=self.parse_manga)

    def parse_manga(self, response: Response, **kwargs: Any) -> Any:
        for script in cast(Any, response.css("script::text").getall()):
            m = re.search(r"JSON.parse\(\"(.*?)\"\);", script)
            if m is not None:
                json_meta = bytes(m.group(1), "utf-8").decode("unicode_escape")
                dict_meta = json.loads(json_meta)
                item = NHentaiMangaSpiderItem.from_json_obj(dict_meta)
                item.num_favorites = get_num_favorites(response=response)
                yield item
                break
