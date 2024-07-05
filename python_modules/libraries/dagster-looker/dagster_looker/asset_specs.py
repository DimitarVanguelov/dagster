from pathlib import Path
from typing import Optional, Sequence

from dagster import AssetSpec
from dagster._annotations import experimental

from .asset_utils import (
    build_looker_dashboard_specs,
    build_looker_explore_specs,
    build_looker_view_specs,
)
from .dagster_looker_translator import DagsterLookerTranslator


@experimental
def build_looker_asset_specs(
    *,
    project_dir: Path,
    dagster_looker_translator: Optional[DagsterLookerTranslator] = None,
) -> Sequence[AssetSpec]:
    """A decorator for defining Looker assets in a project.

    Args:
        project_dir (Path): The path to the Looker project directory.
        dagster_looker_translator (Optional[DagsterLookerTranslator]): Allows customizing how to
            map looker structures to asset keys and asset metadata.

    Examples:
        .. code-block:: python

            from pathlib import Path

            from dagster import external_assets_from_specs
            from dagster_looker import build_looker_asset_specs


            looker_specs = build_looker_asset_specs(project_dir=Path("my_looker_project"))
            looker_assets = external_assets_from_specs(looker_specs)
    """
    dagster_looker_translator = dagster_looker_translator or DagsterLookerTranslator()

    specs = [
        *build_looker_dashboard_specs(project_dir, dagster_looker_translator),
        *build_looker_explore_specs(project_dir, dagster_looker_translator),
        *build_looker_view_specs(project_dir, dagster_looker_translator),
    ]

    return specs
