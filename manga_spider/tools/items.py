from attr import has
from manga_spider.utils import result_files, result_dir
from manga_spider.items import MangaSpiderItem, item_from_json
from typing import Callable, Any, TextIO, BinaryIO
from pathlib import Path
import json
import mmap
import tqdm
import sys


def count_lines(file_path: Path) -> int:
    with file_path.open("r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)
        lines = 0
        while mm.readline():
            lines += 1
        mm.close()
        return lines
    
def mmap_all(file: BinaryIO):
    if sys.platform != "win32":
        return mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ, prot=mmap.PROT_READ)
    else:
        return mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)
    
def item_to_json(item: Any) -> str | None:
    if item is not None:
        if hasattr(item, "to_json") and callable(item.to_json):
            return item.to_json(separators=(",", ":"))
        else:
            return json.dumps(item, separators=(",", ":"))
    else:
        return None

class ItemReWriter:
    __spider: str
    __batch_count: int
    __current_batch_id: int
    __current_item_id: int
    __current_file: TextIO | None

    def __init__(self, spider: str, batch_count: int) -> None:
        if not spider:
            raise ValueError("spider is required")
        if batch_count < 0:
            raise ValueError("batch count must be greater then 0")
        self.__spider = spider
        self.__batch_count = batch_count
        self.__current_batch_id = 0
        self.__current_item_id = 0
        self.__current_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._complete_flie()
        res_dir = result_dir(self.__spider)
        if exc_value is not None:
            for batch_id in range(self.__current_batch_id):
                file_path = res_dir.joinpath(f"item-{batch_id + 1}.jsonl.tmp")
                file_path.unlink(missing_ok=True)
        else:
            for batch_id in range(self.__current_batch_id):
                backup_path = res_dir.joinpath(f"item-{batch_id + 1}.jsonl.backup")
                backup_path.unlink(missing_ok=True)
                original_path = res_dir.joinpath(f"item-{batch_id + 1}.jsonl")
                if original_path.exists():
                    original_path.rename(
                        res_dir.joinpath(f"item-{batch_id + 1}.jsonl.backup")
                    )
                new_path = res_dir.joinpath(f"item-{batch_id + 1}.jsonl.tmp")
                if new_path.exists():
                    new_path.rename(res_dir.joinpath(f"item-{batch_id + 1}.jsonl"))

    def _complete_flie(self):
        if self.__current_file is not None:
            self.__current_file.close()
            self.__current_file = None
            self.__current_item_id = 0
            self.__current_batch_id += 1

    def append_line(self, line: str):
        if self.__current_file is None:
            assert self.__current_item_id == 0
            dir = result_dir(self.__spider)
            self.__current_file = dir.joinpath(
                f"item-{self.__current_batch_id + 1}.jsonl.tmp"
            ).open(mode="w", encoding="utf-8")
        self.__current_file.writelines([line])
        self.__current_item_id += 1
        if self.__batch_count > 0 and self.__current_item_id == self.__batch_count:
            self._complete_flie()


def recreate_items(
    spider: str,
    target_batch_count: int,
    mapper: Callable[[MangaSpiderItem], Any] | None,
):
    with ItemReWriter(spider=spider, batch_count=target_batch_count) as w:
        for res_path in result_files(spider=spider):
            lines = count_lines(res_path)
            with tqdm.tqdm(total=lines, desc=f"Processing {res_path.name}:") as pbar:
                with res_path.open("r+b") as f:
                    with mmap_all(f) as mm:
                        for bline in iter(mm.readline, b""):
                            line = bline.decode("utf-8")
                            if mapper is not None:
                                item = item_from_json(spider=spider, json_str=line)
                                item = mapper(item)
                                if item is not None:
                                    new_line = item_to_json(item=item)
                                    if new_line is not None:
                                        w.append_line(line=new_line + '\n')
                            else:
                                if line.endswith("\r\n"):
                                    line = line[:-2] + "\n"
                                w.append_line(line=line)
                            pbar.update()


def repartition_items(spider: str, batch_count: int):
    recreate_items(spider=spider, target_batch_count=batch_count, mapper=None)

def fix_items(spider: str, batch_count: int, update_num_favorites: bool):
    num_favorites: dict[int, int] = {}
    if update_num_favorites:
        if spider != "nhentai":
            raise ValueError("Only nhentai spider support update num favorites")
        for res_path in result_files(spider=f"{spider}_num_favorites"):
            with res_path.open(encoding="utf-8") as f:
                for line in f:
                    item = item_from_json(spider=spider, json_str=line)
                    num_favorites[item.id] = item.num_favorites
    def fix(item: MangaSpiderItem) -> MangaSpiderItem | None:
        if update_num_favorites:
            item.num_favorites = num_favorites.get(item.id, 0)
        return item if item.is_completed() else None
    recreate_items(spider=spider, target_batch_count=batch_count, mapper=fix)
