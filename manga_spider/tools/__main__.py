import argparse
import argcomplete
from .items import fix_items, repartition_items
from manga_spider.utils import get_batch_count

def fix(args: argparse.Namespace):
    batch_count = args.batch_count if args.batch_count >= 0 else get_batch_count()
    fix_items(args.spider, batch_count, update_num_favorites=args.update_num_favorites)
    
def repartition(args: argparse.Namespace):
    batch_count = args.batch_count if args.batch_count >= 0 else get_batch_count()
    repartition_items(args.spider, batch_count)

parser = argparse.ArgumentParser(
    prog="manga spider tools",
    description="The tools for manga spider",
)
subparsers = parser.add_subparsers()

parser_fix = subparsers.add_parser("fix")
parser_fix.add_argument("--spider", required=True, choices=["nhentai"], help="The name of the spider")
parser_fix.add_argument("--batch-count", dest="batch_count", required=False, type=int, default=-1, help="target batch count")
parser_fix.add_argument("--update-num-favorites", dest="update_num_favorites", action="store_true", help="whether to update the num favorites using num favorites spider results")
parser_fix.set_defaults(func=fix)

parser_repartition = subparsers.add_parser("repartition")
parser_repartition.add_argument("--spider", required=True, choices=["nhentai"], help="The name of the spider")
parser_repartition.add_argument("--batch_count", required=False, type=int, default=-1, help="target batch count")
parser_repartition.set_defaults(func=repartition)

argcomplete.autocomplete(parser)
args = parser.parse_args()
args.func(args)