import {Box} from '@dagster-io/ui-components';
import React, {useContext} from 'react';

import {GraphNode} from './Utils';
import {CloudOSSContext} from '../app/CloudOSSContext';
import {FeatureFlag, featureEnabled} from '../app/Flags';
import {AssetGroupSelector, AssetOwner, ChangeReason, DefinitionTag} from '../graphql/types';
import {useFilters} from '../ui/Filters';
import {useAssetGroupFilter} from '../ui/Filters/useAssetGroupFilter';
import {useAssetOwnerFilter, useAssetOwnersForAssets} from '../ui/Filters/useAssetOwnerFilter';
import {useAssetTagFilter, useAssetTagsForAssets} from '../ui/Filters/useAssetTagFilter';
import {useChangedFilter} from '../ui/Filters/useChangedFilter';
import {useCodeLocationFilter} from '../ui/Filters/useCodeLocationFilter';
import {
  useAssetKindTagsForAssets,
  useComputeKindTagFilter,
} from '../ui/Filters/useComputeKindTagFilter';
import {FilterObject, FilterTag, FilterTagHighlightedText} from '../ui/Filters/useFilter';
import {WorkspaceContext} from '../workspace/WorkspaceContext';

type OptionalFilters =
  | {
      assetGroups?: null;
      setGroupFilters?: null;
      visibleAssetGroups?: null;
      computeKindTags?: null;
      setComputeKindTags?: null;
      changedInBranch?: null;
      setChangedInBranch?: null;
      allAssetTags?: null;
      assetTags?: null;
      setAssetTags?: null;
      allOwners?: null;
      owners?: null;
      setOwners: null;
    }
  | {
      assetGroups?: AssetGroupSelector[];
      visibleAssetGroups?: AssetGroupSelector[];
      setGroupFilters?: (groups: AssetGroupSelector[]) => void;
      computeKindTags?: string[];
      setComputeKindTags?: (s: string[]) => void;
      changedInBranch?: ChangeReason[];
      setChangedInBranch?: (s: ChangeReason[]) => void;
      assetTags?: DefinitionTag[];
      setAssetTags?: (s: DefinitionTag[]) => void;
      owners?: AssetOwner[];
      setOwners?: (owners: AssetOwner[]) => void;
    };

type Props = {
  nodes: GraphNode[];
  clearExplorerPath: () => void;
  explorerPath: string;
  isGlobalGraph: boolean;
} & OptionalFilters;

export function useAssetGraphExplorerFilters({
  nodes,
  isGlobalGraph,
  assetGroups,
  visibleAssetGroups,
  setGroupFilters,
  computeKindTags,
  setComputeKindTags,
  explorerPath,
  clearExplorerPath,
  changedInBranch,
  setChangedInBranch,
  assetTags,
  setAssetTags,
  owners,
  setOwners,
}: Props) {
  const allAssetTags = useAssetTagsForAssets(nodes);

  const {allRepos} = useContext(WorkspaceContext);

  const reposFilter = useCodeLocationFilter();

  const changedFilter = useChangedFilter({changedInBranch, setChangedInBranch});
  const groupsFilter = useAssetGroupFilter({visibleAssetGroups, assetGroups, setGroupFilters});

  const allComputeKindTags = useAssetKindTagsForAssets(nodes);

  const kindTagsFilter = useComputeKindTagFilter({
    allComputeKindTags,
    computeKindTags,
    setComputeKindTags,
  });

  const tagsFilter = useAssetTagFilter({
    allAssetTags,
    tags: assetTags,
    setTags: setAssetTags,
  });

  const allAssetOwners = useAssetOwnersForAssets(nodes);
  const ownerFilter = useAssetOwnerFilter({
    allAssetOwners,
    owners,
    setOwners,
  });

  const filters: FilterObject[] = [];

  if (allRepos.length > 1 && isGlobalGraph) {
    filters.push(reposFilter);
  }
  if (assetGroups) {
    filters.push(groupsFilter);
  }
  const {isBranchDeployment} = React.useContext(CloudOSSContext);
  if (
    changedInBranch &&
    featureEnabled(FeatureFlag.flagExperimentalBranchDiff) &&
    isBranchDeployment
  ) {
    filters.push(changedFilter);
  }
  if (setComputeKindTags) {
    filters.push(kindTagsFilter);
  }
  if (setAssetTags) {
    filters.push(tagsFilter);
  }
  if (setOwners) {
    filters.push(ownerFilter);
  }
  const {button, activeFiltersJsx} = useFilters({filters});
  if (!filters.length) {
    return {button: null, activeFiltersJsx: null};
  }

  return {
    button,
    filterBar:
      activeFiltersJsx.length || explorerPath ? (
        <Box padding={{vertical: 8, horizontal: 12}} flex={{gap: 12}}>
          {' '}
          {activeFiltersJsx}
          {explorerPath ? (
            <FilterTag
              label={
                <Box flex={{direction: 'row', alignItems: 'center'}}>
                  Asset selection is&nbsp;
                  <FilterTagHighlightedText tooltipText={explorerPath}>
                    {explorerPath}
                  </FilterTagHighlightedText>
                </Box>
              }
              onRemove={clearExplorerPath}
            />
          ) : null}
        </Box>
      ) : null,
  };
}
