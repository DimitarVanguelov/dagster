from pathlib import Path

from dagster import Definitions, asset
from dagster._core.definitions.metadata import (
    AnchorBasedFilePathMapping,
    link_to_git,
    with_source_code_references,
)


@asset
def my_asset(): ...


@asset
def another_asset(): ...


defs = Definitions(
    assets=link_to_git(
        assets_defs=with_source_code_references([my_asset, another_asset]),
        git_url="https://github.com/dagster-io/dagster",
        git_branch="main",
        file_path_mapping=AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__).parent, file_anchor_path_in_repository="/"
        ),
    )
)
