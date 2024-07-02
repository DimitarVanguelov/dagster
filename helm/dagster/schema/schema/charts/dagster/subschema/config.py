from typing import Union

from pydantic import Extra, BaseModel


class Source(BaseModel):
    env: str

    class Config:
        extra = Extra.forbid


StringSource = Union[str, Source]
IntSource = Union[int, Source]
