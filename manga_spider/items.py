# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
import abc
from datetime import datetime, timezone
from typing import Any, cast
from typing_extensions import Self
import random
from marshmallow import fields

EXTS = {
    "j": "jpg",
    "p": "png",
    "g": "gif",
}

MEDIA_SERVERS = [3, 5, 7]


@dataclass_json
@dataclass
class MangaImage:
    t: str = field(default_factory=lambda: "")
    w: int = field(default=0)
    h: int = field(default=0)

    @classmethod
    def from_json_obj(cls, obj: Any) -> "Self | None":
        if obj is None:
            return None
        image = cls()
        if "t" in obj and obj["t"] is not None:
            image.t = obj["t"]
        if "w" in obj and obj["w"] is not None:
            image.w = int(obj["w"])
        if "h" in obj and obj["h"] is not None:
            image.h = int(obj["h"])
        return image


@dataclass_json
@dataclass
class MangaImages:
    cover: MangaImage | None = field(default=None)
    pages: list[MangaImage] = field(default_factory=list)
    thumbnail: MangaImage | None = field(default=None)

    @classmethod
    def from_json_obj(cls, obj: Any) -> "Self | None":
        if obj is None:
            return None
        images = cls()
        if "cover" in obj:
            images.cover = MangaImage.from_json_obj(obj["cover"])
        if "pages" in obj and obj["pages"] is not None:
            for page in obj["pages"]:
                image = MangaImage.from_json_obj(page)
                assert image is not None
                images.pages.append(image)
        if "thumbnail" in obj:
            images.thumbnail = MangaImage.from_json_obj(obj["thumbnail"])
        return images


def process_tag(tag: str) -> list[str]:
    return list(dict.fromkeys([part.strip() for part in tag.split("|")]))


def is_downloaded(page: Any) -> bool:
    if page is None or "status" not in page:
        return False
    return page["status"] == "downloaded"


def is_completed(item: Any) -> bool:
    return (
        "download_pages" in item
        and item["download_pages"] is not None
        and (
            "pages" in item
            and item["pages"] is not None
            and (
                sum(1 for page in item["download_pages"] if is_downloaded(page))
                == item["pages"]
            )
        )
    )


@dataclass_json
@dataclass
class MangaSpiderItem:
    id: int
    media_id: str
    title_english: str | None = field(default=None)
    title_japanese: str | None = field(default=None)
    title_pretty: str | None = field(default=None)
    images: MangaImages | None = field(default=None)
    download_pages: list[Any] = field(default_factory=list)
    parodies: list[str] = field(default_factory=list)
    characters: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    artists: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    pages: int = field(default=0)
    upload_date: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format="iso"),
        ),
    )
    num_favorites: int = field(default=0)

    @classmethod
    def from_json_obj(cls, obj: Any) -> "Self":
        item = cls(id=int(obj["id"]), media_id=str(obj["media_id"]))
        if "title" in obj and obj["title"] is not None:
            title = obj["title"]
            if "english" in title and title["english"] is not None:
                item.title_english = title["english"]
            if "japanese" in title and title["japanese"] is not None:
                item.title_japanese = title["japanese"]
            if "pretty" in title and title["pretty"] is not None:
                item.title_pretty = title["pretty"]
        if "images" in obj:
            item.images = MangaImages.from_json_obj(obj["images"])
        if "num_pages" in obj and obj["num_pages"] is not None:
            item.pages = obj["num_pages"]
        else:
            item.pages = len(item.images.pages) if item.images is not None else 0
        if "tags" in obj and obj["tags"] is not None:
            for tag in obj["tags"]:
                if "type" in tag:
                    type = tag["type"]
                    if type == "parody":
                        if "name" in tag:
                            item.parodies.extend(process_tag(tag["name"]))
                    elif type == "character":
                        if "name" in tag:
                            item.characters.extend(process_tag(tag["name"]))
                    elif type == "tag":
                        if "name" in tag:
                            item.tags.extend(process_tag(tag["name"]))
                    elif type == "artist":
                        if "name" in tag:
                            item.artists.extend(process_tag(tag["name"]))
                    elif type == "group":
                        if "name" in tag:
                            item.groups.extend(process_tag(tag["name"]))
                    elif type == "language":
                        if "name" in tag:
                            item.languages.extend(process_tag(tag["name"]))
                    elif type == "category":
                        if "name" in tag:
                            item.categories.extend(process_tag(tag["name"]))
        if "upload_date" in obj and obj["upload_date"] is not None:
            item.upload_date = datetime.fromtimestamp(obj["upload_date"])
        if "num_favorites" in obj and obj["num_favorites"] is not None:
            item.num_favorites = obj["num_favorites"]
        return item

    @abc.abstractmethod
    def type(self) -> str:
        pass

    @abc.abstractmethod
    def page_urls(self) -> list[str]:
        pass

    @abc.abstractmethod
    def page_file_name(self, url: str) -> str:
        pass

    def is_completed(self) -> bool:
        if self.pages == 0:
            return True
        else:
            return (
                self.download_pages is not None
                and sum(1 for page in self.download_pages if is_downloaded(page))
                == self.pages
            )


@dataclass_json
@dataclass
class NHentaiMangaSpiderItem(MangaSpiderItem):

    def type(self) -> str:
        return "nhentai"

    def page_urls(self) -> list[str]:
        if self.images is not None:
            return [
                f"https://i{random.choice(MEDIA_SERVERS)}.nhentai.net/galleries/{self.media_id}/{i + 1}.{EXTS[page.t]}"
                for i, page in enumerate(self.images.pages)
            ]
        else:
            return []

    def page_file_name(self, url: str) -> str:
        return url.rsplit("/", 1)[1]


def item_from_json(spider: str, json_str: str, strict: bool = True) -> MangaSpiderItem:
    if spider == "nhentai":
        item: NHentaiMangaSpiderItem = (
            cast(Any, NHentaiMangaSpiderItem).schema().loads(json_str)
            if strict
            else cast(Any, NHentaiMangaSpiderItem).from_json(json_str)
        )
        return item
    else:
        raise ValueError(f"unknown spider {spider}")
