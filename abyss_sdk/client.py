import requests
from typing import Dict, List


class AbyssClient:
    def __init__(self, funcx_token, transfer_token,
                 abyss_address="https://127.0.0.1:5000"):
        self.abyss_address = abyss_address
        self.funcx_token = funcx_token
        self.transfer_token = transfer_token
        self.headers = {
            'Authorization': f"Bearer {funcx_token}",
            'Transfer': transfer_token,
            'FuncX': f"{funcx_token}"
        }

    def launch(self, compressed_files: Dict[List],
               globus_source_eid: str,
               worker_params: List[Dict], **kwargs) -> str:
        abyss_url = f"{self.abyss_address}/launch"
        payload = {
            "compressed_files": compressed_files,
            "globus_source_eid": globus_source_eid,
            "worker_params": worker_params,
            "transfer_token": self.transfer_token
                   }
        payload.update(kwargs)
        response = requests.post(abyss_url, json=payload,
                                 headers=self.headers)

        abyss_id = response.text

        return abyss_id

    def get_status(self, abyss_id: str) -> dict:
        abyss_url = f"{self.abyss_address}/get_status"
        payload = {
            "abyss_id": abyss_id
        }
        response = requests.get(abyss_url, json=payload,
                                headers=self.headers)

        abyss_status = response.json

        return abyss_status

    def get_metadata(self, abyss_id: str) -> dict:
        abyss_url = f"{self.abyss_address}/get_metadata"
        payload = {
            "abyss_id": abyss_id
        }
        response = requests.get(abyss_url, json=payload,
                                headers=self.headers)

        metadata = response.json

        return metadata
