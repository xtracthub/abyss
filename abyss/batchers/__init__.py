from abyss.batchers.batcher import Batcher
from abyss.batchers.knapsack_batcher import KnapsackBatcher
from abyss.batchers.mmd_batcher import MMDBatcher
from abyss.batchers.round_robin_batcher import RoundRobinBatcher

BATCHER_NAME_MAPPING = {
    "round_robin": RoundRobinBatcher,
    "knapsack": KnapsackBatcher,
    "mmd": MMDBatcher
}


def get_batcher(batcher_name: str) -> Batcher:
    """Returns a batcher by name.

    Parameters
    ----------
    batcher_name : str
        Name of batcher to return.

    Returns
    -------
    Batcher
        File batcher.
    """
    try:
        return BATCHER_NAME_MAPPING[batcher_name]
    except KeyError:
        raise ValueError(f"{batcher_name} batcher does not exist")