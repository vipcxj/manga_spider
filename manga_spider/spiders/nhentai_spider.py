import math
from typing import Any, Iterable, cast
import re
import json
from manga_spider.items import NHentaiMangaSpiderItem
from manga_spider.utils import completed_ids, results_each_line

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
        
MODE = [
    "default",
    "same_artist"
]

def artist_to_url(artist: str, page: int) -> str:
    tag = re.sub(r"[\s|]+", "-", artist)
    return f"https://nhentai.net/artist/{tag}/?page={page}"

class NHentaiSpider(scrapy.Spider):
    mode: str | None
    exclude_ids: set[int]
    name = "nhentai"
    
    def __init__(self, name: str | None = None, **kwargs: Any):
        super().__init__(name, **kwargs)
        self.exclude_ids = completed_ids(self.name)
    
    def start_requests(self) -> Iterable[scrapy.Request]:
        if hasattr(self, "mode") and self.mode == "same_artist":
            artists: set[str] = set()
            def collect_artists(line: str) -> bool:
                item = json.loads(line)
                if "artists" in item and item["artists"] is not None:
                    artists.update(item["artists"])
                # item = item_from_json(spider=self.name, json_str=line, strict=False)
                return True
            results_each_line(spider=self.name, cb=collect_artists, use_tqdm=True, tqdm_desc="Collect artists from {file_name}")
            print(f"Collected {len(artists)} artists")
            for artist in artists:
                yield scrapy.Request(artist_to_url(artist=artist, page=1), callback=self.parse_artists, cb_kwargs={"artist": artist, "page": 1})
        else:                
            yield scrapy.Request("https://nhentai.net/", callback=self.parse_home)

    def collect_manga_urls(self, latest_id: int) -> list[str]:
        ids = [id + 1 for id in range(latest_id)]
        return [f"https://nhentai.net/g/{id}/" for id in ids if id not in self.exclude_ids]

    def parse_home(self, response: Response, **kwargs: Any) -> Any:
        latest_link = response.css(
            "#content .index-container:not(.index-popular) div.gallery a"
        ).attrib["href"]
        latest_id = extract_id_from_link(latest_link)
        for url in self.collect_manga_urls(latest_id=latest_id):
            yield scrapy.Request(url=url, callback=self.parse_manga)
            
    def parse_artists(self, response: Response, **kwargs: Any) -> Any:
        count = int(response.css("#content h1 span.count::text").get())
        artist = kwargs["artist"]
        page = kwargs["page"]
        pages = math.ceil(count * 1.0 / 25)
        mangas = response.css("#content > div.container.index-container > div.gallery > a")
        for manga in cast(Any, mangas):
            href = manga.attrib["href"]
            manga_id = extract_id_from_link(href)
            if manga_id not in self.exclude_ids:
                yield scrapy.Request(url=f"https://nhentai.net/g/{manga_id}/", callback=self.parse_manga)
        if page < pages:
            yield scrapy.Request(artist_to_url(artist=artist, page=page + 1), callback=self.parse_artists, cb_kwargs={"artist": artist, "page": page + 1})

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
