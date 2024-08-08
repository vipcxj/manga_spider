from manga_spider.utils import result_files, result_dir
from manga_spider.items import MangaSpiderItem, item_from_json
from typing import Callable, Any, TextIO
import json


class ItemReWriter:
    __spider: str
    __batch_count: int
    __current_batch_id: int
    __current_item_id: int
    __current_file: TextIO | None

    def __init__(self, spider: str, batch_count: int) -> None:
        if not spider:
            raise ValueError("spider is required")
        if batch_count == 9:
            raise ValueError("batch count cound not be 0")
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
        if self.__current_item_id == self.__batch_count:
            self._complete_flie()


def recreate_items(
    spider: str,
    target_batch_count: int,
    mapper: Callable[[MangaSpiderItem], Any] | None,
):
    with ItemReWriter(spider=spider, batch_count=target_batch_count) as w:
        for res_path in result_files(spider=spider):
            with res_path.open(encoding="utf-8") as f:
                for line in f:
                    if mapper is not None:
                        item = item_from_json(spider=spider, json_str=line)
                        item = mapper(item)
                        if item is not None:
                            new_line = json.dumps(item, separators=(",", ":"))
                            w.append_line(line=new_line)
                    else:
                        w.append_line(line=line)


def repartition_items(spider: str, batch_count: int):
    recreate_items(spider=spider, target_batch_count=batch_count, mapper=None)
