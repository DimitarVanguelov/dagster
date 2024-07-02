from typing import Any, Union, Mapping, Optional, cast
from pathlib import Path
from functools import lru_cache

import yaml
import dagster._check as check

SlingReplicationParam = Union[Mapping[str, Any], str, Path]


@lru_cache(maxsize=None)
def read_replication_path(replication_path: Path) -> Mapping[str, Any]:
    """Reads a Sling replication config from a path and returns a dict.

    This function is cached to ensure that we don't read the same path multiple times, which
    creates multiple copies of the parsed manifest in memory.
    """
    return cast(Mapping[str, Any], yaml.safe_load(replication_path.read_bytes()))


def validate_replication(replication: Optional[SlingReplicationParam]) -> Mapping[str, Any]:
    replication = replication or {}
    check.inst_param(replication, "manifest", (Path, str, dict))

    if isinstance(replication, str):
        replication = Path(replication)

    if isinstance(replication, Path):
        # Resolve the path to ensure a consistent key for the cache
        replication = read_replication_path(replication.resolve())

    return replication
