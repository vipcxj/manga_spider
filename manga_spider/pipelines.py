# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from scrapy.http import Request
from scrapy.http.request import NO_CALLBACK
from manga_spider.items import MangaSpiderItem
import re


class MangaSpiderPipeline:
    def process_item(self, item, spider):
        return item

class MyImagesPipeline(ImagesPipeline):
    
    def get_media_requests(self, item, info):
        if isinstance(item, MangaSpiderItem):
            return [Request(url, callback=NO_CALLBACK) for url in item.page_urls()]
        else:
            return super().get_media_requests(item, info)
    
    def file_path(self, request, response=None, info=None, *, item=None):
        if isinstance(item, MangaSpiderItem):
            return f"{item.type()}/{item.id % 100}/{item.id}/{item.page_file_name(request.url)}"
        else:
            return super().file_path(request, response, info, item=item)