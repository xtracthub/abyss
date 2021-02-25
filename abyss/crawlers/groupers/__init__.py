from abyss.crawlers.groupers.grouper import Grouper
from abyss.crawlers.groupers.extension_grouper import ExtensionGrouper
from abyss.crawlers.groupers.matio_grouper import MatIOGrouper

GROUPER_NAME_MAPPING = {
    "matio": MatIOGrouper,
    "extension": ExtensionGrouper
}


def get_grouper(grouper_name: str) -> Grouper:
    """Returns a groupers by name

    Parameters
    ----------
    grouper_name : str
        Name of groupers to return

    Returns
    -------
    Grouper
        File groupers.
    """
    try:
        return GROUPER_NAME_MAPPING[grouper_name]
    except KeyError:
        # raise ValueError(f"{grouper_name} groupers does not exist")
        pass
