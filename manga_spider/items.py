# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

EXTS = {
    "j": "jpg",
    "p": "png",
    "g": "gif",
}

@dataclass
class MangaImage:
    t: str = field(default_factory=lambda: "")
    w: int = field(default=0)
    h: int = field(default=0)
    
    @staticmethod
    def from_json(obj: Any) -> "MangaImage | None":
        if obj is None:
            return None
        image = MangaImage()
        if "t" in obj and obj["t"] is not None:
            image.t = obj["t"]
        if "w" in obj and obj["w"] is not None:
            image.w = obj["w"]
        if "h" in obj and obj["h"] is not None:
            image.h = obj["h"]
        return image

@dataclass
class MangaImages:
    cover: MangaImage = field(default=None)
    pages: list[MangaImage] = field(default_factory=list)
    thumbnail: MangaImage = field(default=None)
    
    @staticmethod
    def from_json(obj: Any) -> "MangaImages | None":
        if obj is None:
            return None
        images = MangaImages()
        if "cover" in obj:
            images.cover = MangaImage.from_json(obj["cover"])
        if "pages" in obj and obj["pages"] is not None:
            for page in obj["pages"]:
                images.pages.append(MangaImage.from_json(page))
        if "thumbnail" in obj:
            images.thumbnail = MangaImage.from_json(obj["thumbnail"])
        return images
    
def process_tag(tag: str) -> list[str]:
    return list(dict.fromkeys([part.strip() for part in tag.split("|")]))

@dataclass
class MangaSpiderItem:
    id: int
    media_id: str
    title_english: str | None = field(default=None)
    title_japanese: str | None = field(default=None)
    title_pretty: str | None = field(default=None)
    images: MangaImages | None = field(default=None)
    parodies: list[str] = field(default_factory=list)
    characters: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    artists: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    pages: int = field(default=0)
    upload_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @staticmethod
    def from_dict(obj: Any) -> "MangaSpiderItem":
        item = MangaSpiderItem(id=obj["id"], media_id=obj["media_id"])
        if "title" in obj and obj["title"] is not None:
            title = obj["title"]
            if "english" in title and title["english"] is not None:
                item.title_english = title["english"]
            if "japanese" in title and title["japanese"] is not None:
                item.title_japanese = title["japanese"]
            if "pretty" in title and title["pretty"] is not None:
                item.title_pretty = title["pretty"]
        if "images" in obj:
            item.images = MangaImages.from_json(obj["images"])
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
        return item
