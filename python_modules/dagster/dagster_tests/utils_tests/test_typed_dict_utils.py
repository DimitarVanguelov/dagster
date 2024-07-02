from typing import Any, Dict, Optional, TypedDict

from typing_extensions import NotRequired
from dagster._utils.typed_dict import init_optional_typeddict


class MyNestedTypedDict(TypedDict):
    nested: Optional[str]


class MyTypedDict(TypedDict):
    nested: MyNestedTypedDict
    optional_field: Optional[str]
    dict_field: Dict[str, Any]
    not_required_field: NotRequired[str]


def test_init_optional_typeddict():
    assert init_optional_typeddict(MyTypedDict) == {
        "nested": {"nested": None},
        "optional_field": None,
        "dict_field": {},
    }
