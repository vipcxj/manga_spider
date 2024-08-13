from ast import mod
import math
from typing import Any, Iterable, cast
import re
import json
from unittest import mock
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
    "same_artist",
    "same_group",
]

def tag_to_url(tag: str, page: int, type: str) -> str:
    tag = re.sub(r"[\s|]+", "-", tag)
    return f"https://nhentai.net/{type}/{tag}/?page={page}"


class NHentaiSpider(scrapy.Spider):
    mode: str | None = "default"
    exclude_ids: set[int]
    name = "nhentai"
    
    def __init__(self, name: str | None = None, **kwargs: Any):
        super().__init__(name, **kwargs)
        self.exclude_ids = completed_ids(self.name)
    
    def start_requests(self) -> Iterable[scrapy.Request]:
        if hasattr(self, "mode"):
            if self.mode == "same_artist":
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
                    yield scrapy.Request(tag_to_url(tag=artist, page=1, type="artist"), callback=self.parse_tags, cb_kwargs={"tag": artist, "type": "artist", "page": 1})
            elif self.mode == "same_group":
                groups: set[str] = set()
                def collect_groups(line: str) -> bool:
                    item = json.loads(line)
                    if "groups" in item and item["groups"] is not None:
                        groups.update(item["groups"])
                    return True
                results_each_line(spider=self.name, cb=collect_groups, use_tqdm=True, tqdm_desc="Collect groups from {file_name}")
                print(f"Collected {len(groups)} groups")
                for group in groups:
                    yield scrapy.Request(tag_to_url(tag=group, page=1, type="group"), callback=self.parse_tags, cb_kwargs={"tag": group, "type": "group", "page": 1})
            elif self.mode == "default":
                yield scrapy.Request("https://nhentai.net/", callback=self.parse_home)
            else:
                raise ValueError(f"Invalid spider argument mode {self.mode}")
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
            
    def parse_tags(self, response: Response, **kwargs: Any) -> Any:
        count = int(response.css("#content h1 span.count::text").get())
        tag = kwargs["tag"]
        type = kwargs["type"]
        page = kwargs["page"]
        pages = math.ceil(count * 1.0 / 25)
        mangas = response.css("#content > div.container.index-container > div.gallery > a")
        for manga in cast(Any, mangas):
            href = manga.attrib["href"]
            manga_id = extract_id_from_link(href)
            if manga_id not in self.exclude_ids:
                self.exclude_ids.add(manga_id)
                yield scrapy.Request(url=f"https://nhentai.net/g/{manga_id}/", callback=self.parse_manga)
        if page < pages:
            yield scrapy.Request(tag_to_url(tag=tag, page=page + 1, type=type), callback=self.parse_tags, cb_kwargs={"tag": tag, "type": type, "page": page + 1})

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
