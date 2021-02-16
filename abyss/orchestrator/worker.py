class Worker:
    def __init__(self, globus_eid: str, funcx_eid: str,
                 available_space: int):
        """Class for holding worker information.

        Parameters
        ----------
        globus_eid : str
            Globus endpoint ID located at worker.
        funcx_eid : str
            FuncX endpoint ID located at worker.
        available_space : int
            Space available on worker.
        """
        self.globus_eid = globus_eid
        self.funcx_eid = funcx_eid
        self.available_space = available_space
        self.worker_id = self.funcx_eid
