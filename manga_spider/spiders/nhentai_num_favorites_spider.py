from typing import Any, Iterable
import scrapy
from scrapy.http import Response

from manga_spider.utils import completed_ids
from manga_spider.items import NHentaiMangaSpiderItem
from .nhentai_spider import extract_id_from_link, get_num_favorites

class NHentaiNumFavoritesSpider(scrapy.Spider):
    name = "nhentai_num_favorites"
    
    def start_requests(self) -> Iterable[scrapy.Request]:
        for id in completed_ids("nhentai"):
            yield scrapy.Request(f"https://nhentai.net/g/{id}/", dont_filter=True)
            
    def parse(self, response: Response, **kwargs: Any) -> Any:
        id = extract_id_from_link(response.url)
        num_favorites = get_num_favorites(response=response)
        yield NHentaiMangaSpiderItem(id=id, media_id="", num_favorites=num_favorites)
        