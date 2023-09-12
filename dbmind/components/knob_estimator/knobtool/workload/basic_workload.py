import abc


class BasicWorkload(abc.ABC):
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def evaluate(self, sample):
        raise NotImplementedError
