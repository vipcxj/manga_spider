import datetime
from genericpath import exists
import glob
import pathlib
import re
from typing import Any
from scrapy.extensions.feedexport import FeedExporter

from manga_spider import utils

class MyFeedExport(FeedExporter):
    
    def _analyse_uri(self, uri: str, spider, feed_options: Any):
        uri_params = self._get_uri_params(spider, feed_options["uri_params"])
        glob_path = uri.replace("%(batch_time)s", "*").replace("%(batch_id)d", "*")
        glob_path = glob_path % uri_params
        reg_path = uri.replace("%(batch_time)s", "(?P<bt>.+)").replace("%(batch_id)d", "(?P<bi>\\d+)")
        reg_path = reg_path % uri_params
        reg_path = re.escape(reg_path).replace(re.escape("(?P<bt>.+)"), "(?P<bt>.+)").replace(re.escape("(?P<bi>\\d+)"), "(?P<bi>\\d+)")
        max_bi = None
        max_bt = None
        max_path = None
        for path in glob.glob(glob_path):
            m = re.match(reg_path, path)
            if m is not None:
                bi = m.groupdict().get("bi")
                if bi is not None:
                    bi = int(bi)
                    if max_bi is None or bi > max_bi:
                        max_bi = bi
                        max_path = path
                        continue
                bt = m.groupdict().get("bt")
                if bt is not None:
                    bt = datetime.datetime.strptime(bt, "%Y-%m-%dT%H-%M-%S.%f+00-00").replace(tzinfo=datetime.timezone.utc)
                    if max_bt is None or bt > max_bt:
                        max_bt = bt
                        max_path = path
                        continue
        return (max_bi, max_bt, max_path if max_path is not None else uri % uri_params)
    
    def open_spider(self, spider):
        for uri, feed_options in self.feeds.items():
            bi, _, path = self._analyse_uri(uri, spider, feed_options)
            batch_id = bi if bi is not None else 1
            slot = self._start_new_batch(
                batch_id=batch_id,
                uri=path,
                feed_options=feed_options,
                spider=spider,
                uri_template=uri,
            )
            if exists(path):
                slot.itemcount = utils.count_lines(pathlib.Path(path))
                print(f"Resumed from item {batch_id} at pos {slot.itemcount}.")
            elif bi != 1:
                print(f"The item file {path} not exists, something maybe wrong.")
            self.slots.append(slot)