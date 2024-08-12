import contextlib
import mmap
import os
import pathlib
import json
import sys
from typing import Any, BinaryIO, Callable

import tqdm
from manga_spider.settings import FEED_EXPORT_BATCH_ITEM_COUNT, FEEDS

def uri_params(params, spider):
    return {
        **params,
        "spider_name": spider.name
    }
    
def mmap_all(file: BinaryIO):
    if sys.platform != "win32":
        return mmap.mmap(file.fileno(), length=0, prot=mmap.PROT_READ)
    else:
        return mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)
    
def result_dir(spider: str):
    return pathlib.Path(__file__).parent.resolve().joinpath(f"results/{spider}")

def result_file(spider: str, batch_id: int):
    return result_dir(spider=spider).joinpath(f"item-{batch_id}.jsonl")
    
def result_files(spider: str):
    files: list[pathlib.Path] = []
    for file in result_dir(spider=spider).glob("item-*.jsonl"):
        if file.is_file():
            files.append(file)
    return files

def get_feed_store_root(format: str):
    for key, feed in FEEDS.items():
        if "format" in feed and feed["format"] == format:
            return key
    return None

def resolve_feed_store_path(path: str, format: str = "jsonlines"):
    root = get_feed_store_root(format=format)
    if root is None:
        raise ValueError(f"Unable to find the feed config of format {format}.")
    return os.path.join(root, path)

def count_lines(file_path: pathlib.Path) -> int:
    with file_path.open("r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)
        lines = 0
        while mm.readline():
            lines += 1
        mm.close()
        return lines
            
def completed_ids(spider: str):
    ids: set[int] = set()
    for file in result_files(spider=spider):
        with file.open(encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                ids.add(item["id"])
    return ids

def _maybe_tqdm(use_tqdm: bool, tqdm_desc: str | None, calc_totals: Callable[[], int], desc_ctx: dict[str, Any]):
    if use_tqdm:
        if tqdm_desc is not None:
            tqdm_desc = tqdm_desc.format(**desc_ctx)
        return tqdm.tqdm(total=calc_totals(), desc=tqdm_desc)
    else:
        return contextlib.nullcontext()

def results_each_line(spider: str, cb: Callable[[str], bool], use_tqdm: bool = False, tqdm_desc: str | None = None):
    """_summary_
    iterate the line of the result files
    Args:
        spider (str): the spider name
        cb (Callable[[str], bool]): the callback, accept line str. return False to stop iterate.
    """
    for file_path in result_files(spider=spider):
        with _maybe_tqdm(use_tqdm=use_tqdm, tqdm_desc=tqdm_desc, calc_totals=lambda: count_lines(file_path), desc_ctx={"file_name": file_path.name}) as pbar:
            with file_path.open("r+b") as f:
                with mmap_all(f) as mm:
                    for bline in iter(mm.readline, b""):
                        line = bline.decode("utf-8")
                        if not cb(line):
                            break
                        if pbar is not None:
                            pbar.update()
                    
def get_batch_count():
    return FEED_EXPORT_BATCH_ITEM_COUNT