from typing import Any, Iterable
import re
import json
from manga_spider.items import NHentaiMangaSpiderItem
from manga_spider.utils import completed_ids

import scrapy
from scrapy.http import Request, Response
from scrapy.http.request import NO_CALLBACK
from scrapy.utils.defer import maybe_deferred_to_future
    

class NHentaiSpider(scrapy.Spider):
    name = "nhentai"
    start_urls = [
        "https://nhentai.net/" 
    ]       
            
    def collect_manga_urls(self, latest_id: int) -> list[str]:
        ids = [id + 1 for id in range(latest_id)]
        exclude_ids = completed_ids(self.name)
        return [f"https://nhentai.net/g/{id}/" for id in ids if id not in exclude_ids]
    
    def parse(self, response: Response, **kwargs: Any) -> Any:
        latest_link = response.css("#content .index-container:not(.index-popular) div.gallery a").attrib["href"]
        m = re.search(r"/g/(\d+)", latest_link)
        if m is not None:
            latest_id = int(m.group(1))
            for url in self.collect_manga_urls(latest_id=latest_id):
                yield scrapy.Request(url=url, callback=self.parse_manga)
        else:
            raise RuntimeError(f"Unable parse the latest manga id from link: {latest_link}")
    
    def parse_manga(self, response: Response, **kwargs: Any) -> Any:
        for script in response.css("script::text").getall():
            m = re.search(r"JSON.parse\(\"(.*?)\"\);", script)
            if m is not None:
                json_meta = bytes(m.group(1), "utf-8").decode("unicode_escape")
                dict_meta = json.loads(json_meta)
                item = NHentaiMangaSpiderItem.from_dict(dict_meta)
                yield item
                break
