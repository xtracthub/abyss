from abyss.dispatcher.fifo_dispatcher import FIFODispatcher
from abyss.dispatcher.dispatcher import Dispatcher

DISPATCHER_NAME_MAPPING = {
    "fifo": FIFODispatcher,
}


def get_dispatcher(dispatcher_name: str) -> Dispatcher:
    """Returns a dispatcher by name.

    Parameters
    ----------
    dispatcher_name : str
        Name of dispatcher to return.

    Returns
    -------
    Dispatcher
        Member of Dispatcher subclass.
    """
    try:
        return DISPATCHER_NAME_MAPPING[dispatcher_name]
    except KeyError:
        raise ValueError(f"{dispatcher_name} dispatcher does not exist")