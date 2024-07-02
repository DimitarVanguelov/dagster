from abc import ABC
from typing import Any, Type, TypeVar, Optional, Sequence
from inspect import Parameter

from typing_extensions import Annotated

from dagster._seven import is_subclass
from dagster._core.decorator_utils import get_type_hints, get_function_params
from dagster._core.definitions.resource_definition import ResourceDefinition


def get_resource_args(fn) -> Sequence[Parameter]:
    type_annotations = get_type_hints(fn)
    return [
        param
        for param in get_function_params(fn)
        if _is_resource_annotation(type_annotations.get(param.name))
    ]


RESOURCE_PARAM_METADATA = "resource_param"


class TreatAsResourceParam(ABC):
    """Marker class for types that can be used as a parameter on an annotated
    function like `@asset`. Any type marked with this class does not require
    a ResourceParam when used on an asset.

    Example:
        class YourClass(TreatAsResourceParam):
            ...

        @asset
        def an_asset(your_class: YourClass):
            ...
    """


def _is_resource_annotation(annotation: Optional[Type[Any]]) -> bool:
    from dagster._config.pythonic_config import ConfigurableResourceFactory

    if isinstance(annotation, type) and (
        is_subclass(annotation, ResourceDefinition)
        or is_subclass(annotation, ConfigurableResourceFactory)
        or is_subclass(annotation, TreatAsResourceParam)
    ):
        return True

    return hasattr(annotation, "__metadata__") and getattr(annotation, "__metadata__") == (
        RESOURCE_PARAM_METADATA,
    )


T = TypeVar("T")
ResourceParam = Annotated[T, RESOURCE_PARAM_METADATA]
