from xtract_sdk.packagers import Family
from abyss.crawlers.groupers import Grouper


class SimpleGrouper(Grouper):
    def group(self, file_ls) -> [Family]:
        pass
