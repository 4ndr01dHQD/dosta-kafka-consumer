from abc import ABC

from pydantic import BaseModel


class AbstractHandler(ABC):

    VALIDATOR_CLASS: type[BaseModel] = NotImplemented

    @classmethod
    def validate_data(cls, data: dict) -> BaseModel:
        return cls.VALIDATOR_CLASS(**data)

    @classmethod
    def handle(cls, key: str, data: BaseModel) -> dict:
        raise NotImplemented
