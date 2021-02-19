from abyss.batchers.batcher import Batcher
from abyss.batchers.round_robin_batcher import RoundRobinBatcher
from abyss.batchers.knapsack_batcher import KnapsackBatcher
from abyss.batchers.mmd_batcher import MMDBatcher

BATCHER_NAME_MAPPING = {
    "round_robin": RoundRobinBatcher,
    "knapsack": KnapsackBatcher,
    "mmd": MMDBatcher
}


def get_batcher(batcher_name: str) -> Batcher:
    """Returns a groupers by name

    Parameters
    ----------
    batcher_name : str
        Name of groupers to return

    Returns
    -------
    Grouper
        File groupers.
    """
    try:
        return BATCHER_NAME_MAPPING[batcher_name]
    except KeyError:
        raise ValueError(f"{batcher_name} batcher does not exist")