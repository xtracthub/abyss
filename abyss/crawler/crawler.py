from abc import ABC, abstractmethod


class Crawler(ABC):
    @abstractmethod
    def crawl(self):
        raise NotImplementedError
