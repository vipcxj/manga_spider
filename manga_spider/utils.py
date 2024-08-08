import pathlib
import json
from os import listdir

def uri_params(params, spider):
    return {
        **params,
        "spider_name": spider.name
    }
    
def result_files(spider: str):
    res_path = pathlib.Path(__file__).parent.resolve().joinpath(f"results/{spider}")
    files: list[pathlib.Path] = []
    for file in res_path.glob("item-*.jsonl"):
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
                    