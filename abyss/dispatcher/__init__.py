from abyss.dispatcher.fifo_dispatcher import FIFODispatcher

DISPATCHER_NAME_MAPPING = {
    "fifo": FIFODispatcher,
}


def get_dispatcher(dispatcher_name):
    try:
        return DISPATCHER_NAME_MAPPING[dispatcher_name]
    except KeyError:
        raise ValueError(f"{dispatcher_name} dispatcher does not exist")