from abyss.grouper.grouper import Grouper
from abyss.grouper.extension_grouper import ExtensionGrouper
from abyss.grouper.matio_grouper import MatIOGrouper

GROUPER_NAME_MAPPING = {
    "matio": MatIOGrouper,
    "extension": ExtensionGrouper
}


def get_grouper(grouper_name: str) -> Grouper:
    """Returns a grouper by name

    Parameters
    ----------
    grouper_name : str
        Name of grouper to return

    Returns
    -------
    Grouper
        File grouper.
    """
    try:
        return GROUPER_NAME_MAPPING[grouper_name]
    except KeyError:
        raise ValueError(f"{grouper_name} grouper does not exist")
