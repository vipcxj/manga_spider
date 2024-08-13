from genericpath import exists
import os
import pathlib
import tarfile
from manga_spider.utils import (
    count_lines,
    mmap_all,
    result_files,
    result_dir,
    result_file,
    resolve_feed_store_path,
)
from manga_spider.items import item_from_json, is_completed
from typing import Callable, Any, TextIO
import json
import tqdm


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
    mapper: Callable[[dict[str, Any]], Any] | None,
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
                                item = json.loads(line)
                                item = mapper(item)
                                if item is not None:
                                    new_line = json.dumps(item, separators=(",", ":"))
                                    if new_line is not None:
                                        w.append_line(line=new_line + "\n")
                            else:
                                if line.endswith("\r\n"):
                                    line = line[:-2] + "\n"
                                w.append_line(line=line)
                            pbar.update()


def repartition_items(spider: str, batch_count: int):
    recreate_items(spider=spider, target_batch_count=batch_count, mapper=None)


def fix_items(
    spider: str, batch_count: int, update_num_favorites: bool, unique_images: bool
):
    num_favorites: dict[int, int] = {}
    images: dict[str, str] = {}
    dups = 0
    if update_num_favorites:
        if spider != "nhentai":
            raise ValueError("Only nhentai spider support update num favorites")
        for res_path in result_files(spider=f"{spider}_num_favorites"):
            with res_path.open(encoding="utf-8") as f:
                for line in f:
                    item = json.loads(line)
                    num_favorites[item["id"]] = item["num_favorites"]

    def fix(item: dict[str, Any]) -> Any:
        if is_completed(item=item):
            if update_num_favorites:
                item["num_favorites"] = num_favorites.get(item["id"], 0)
            if unique_images:
                for page in item["download_pages"]:
                    checksum = page["checksum"]
                    if checksum in images:
                        image_path = resolve_feed_store_path(page["path"])
                        if os.path.exists(image_path):
                            os.remove(image_path)
                        page["path"] = images[checksum]
                        dups += 1
                    else:
                        images[checksum] = page["path"]
            return item
        else:
            return None

    recreate_items(spider=spider, target_batch_count=batch_count, mapper=fix)
    if unique_images:
        print(
            f"Found {len(images)} unique images, and {dups} duplicate images removed."
        )


def tar_images(spider: str, batch_id: int, dest: str | None = None, move: bool = False):
    file_path = result_file(spider=spider, batch_id=batch_id)
    if file_path.exists():
        if dest is None:
            dest = f"{file_path.stem}.tar.gz"
        else:
            dest = dest.format(batch_id=batch_id)
        tar_path = pathlib.Path.cwd().joinpath(dest)
        with tarfile.open(tar_path, mode="w:gz") as tar_file:
            lines = count_lines(file_path)
            with tqdm.tqdm(total=lines, desc=f"Processing {file_path.name}") as pbar:
                with file_path.open("r+b") as f:
                    with mmap_all(f) as mm:
                        for bline in iter(mm.readline, b""):
                            line = bline.decode("utf-8")
                            item = item_from_json(
                                spider=spider, json_str=line, strict=False
                            )
                            if item is not None and item.is_completed():
                                for path in [
                                    page["path"] for page in item.download_pages
                                ]:
                                    abs_path = resolve_feed_store_path(path=path)
                                    if exists(abs_path):
                                        tar_file.add(abs_path, arcname=path)
                                        if move:
                                            os.remove(abs_path)
                            pbar.update()
            print(f"Tar file of item {batch_id} exported -> {tar_path}")
    else:
        print(
            f'The result file with batch id {batch_id} not exist. The expected file path is "f{file_path.as_uri()}".'
        )
