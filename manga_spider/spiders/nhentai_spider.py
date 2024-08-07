from typing import Any, Iterable
import re
import json
from manga_spider.items import MangaSpiderItem

import scrapy
from scrapy.http import Request, Response
    

class NHentaiSpider(scrapy.Spider):
    name = "nhentai"
    
    def start_requests(self) -> Iterable[Request]:
        urls = [
            f"https://nhentai.net/g/{i}/" for i in range(1, 523112)
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        return super().start_requests()
    
    def process_tag(self, tags: list[str]) -> list[str]:
        return list(dict.fromkeys([part.strip() for tag in tags for part in tag.split("|")]))
    
    def parse(self, response: Response, **kwargs: Any) -> Any:
        for script in response.css("script::text").getall():
            m = re.search(r"JSON.parse\(\"(.*?)\"\);", script)
            if m is not None:
                json_meta = bytes(m.group(1), "utf-8").decode("unicode_escape")
                dict_meta = json.loads(json_meta)
                item = MangaSpiderItem.from_dict(dict_meta)
                yield item
                break
