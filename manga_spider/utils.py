import pathlib
import json
from os import listdir
from manga_spider.settings import FEED_EXPORT_BATCH_ITEM_COUNT

def uri_params(params, spider):
    return {
        **params,
        "spider_name": spider.name
    }
    
def result_dir(spider: str):
    return pathlib.Path(__file__).parent.resolve().joinpath(f"results/{spider}")
    
def result_files(spider: str):
    files: list[pathlib.Path] = []
    for file in result_dir(spider=spider).glob("item-*.jsonl"):
        if file.is_file():
            files.append(file)
    return files
            
def completed_ids(spider: str):
    ids: set[int] = set()
    for file in result_files(spider=spider):
        with file.open(encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                ids.add(item["id"])
    return ids
                    
def get_batch_count():
    return FEED_EXPORT_BATCH_ITEM_COUNT