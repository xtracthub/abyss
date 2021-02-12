from xtract_sdk.packagers import Group, Family
from abyss.grouper.grouper import Grouper


class SimpleGrouper(Grouper):
    def group(self, file_ls) -> [Family]:
        pass
