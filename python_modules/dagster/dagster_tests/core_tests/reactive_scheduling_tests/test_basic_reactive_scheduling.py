from typing import Set
from uuid import uuid4

import pendulum
from dagster import asset
from dagster._core.definitions.asset_dep import AssetDep
from dagster._core.definitions.data_version import DataVersion
from dagster._core.definitions.decorators.source_asset_decorator import observable_source_asset
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.materialize import materialize
from dagster._core.definitions.observe import observe
from dagster._core.definitions.partition import (
    StaticPartitionsDefinition,
)
from dagster._core.definitions.partition_mapping import StaticPartitionMapping
from dagster._core.definitions.time_window_partitions import (
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    TimeWindow,
)
from dagster._core.execution.context.compute import AssetExecutionContext
from dagster._core.instance import DagsterInstance
from dagster._core.reactive_scheduling.asset_graph_view import (
    AssetPartition,
    PartitionKey,
)
from dagster._core.reactive_scheduling.expr import Expr
from dagster._core.reactive_scheduling.scheduling_plan import (
    OnAnyNewParentUpdated,
    ReactiveSchedulingPlan,
    RulesLogic,
    build_reactive_scheduling_plan,
)
from dagster._core.reactive_scheduling.scheduling_policies import DefaultSchedulingPolicy
from dagster._core.reactive_scheduling.scheduling_policy import (
    SchedulingExecutionContext,
    SchedulingPolicy,
)

from .test_policies import (
    AlwaysIncludeSchedulingPolicy,
    NeverIncludeSchedulingPolicy,
    build_test_context,
    slices_equal,
)


def test_include_scheduling_policy() -> None:
    assert SchedulingPolicy


def test_scheduling_policy_parameter() -> None:
    scheduling_policy = SchedulingPolicy()

    @asset(scheduling_policy=scheduling_policy)
    def an_asset() -> None:
        raise Exception("never executed")

    assert an_asset.scheduling_policies_by_key[AssetKey(["an_asset"])] is scheduling_policy

    defs = Definitions([an_asset])
    ak = AssetKey(["an_asset"])
    assert defs.get_assets_def(ak).scheduling_policies_by_key[ak] is scheduling_policy


def test_create_scheduling_execution_context() -> None:
    defs = Definitions([])

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)

    assert context
    assert context.queryer
    assert context.instance
    assert context.instance is instance


def test_partition_space() -> None:
    letters_static_partition_def = StaticPartitionsDefinition(["A", "B", "C"])
    numbers_static_partition_def = StaticPartitionsDefinition(["1", "2", "3"])

    @asset(partitions_def=letters_static_partition_def)
    def up_letters() -> None:
        ...

    letter_to_number_mapping = StaticPartitionMapping({"A": "1", "B": "2", "C": "3"})

    @asset(
        deps=[AssetDep(up_letters, partition_mapping=letter_to_number_mapping)],
        partitions_def=numbers_static_partition_def,
    )
    def down_numbers() -> None:
        ...

    defs = Definitions([up_letters, down_numbers])

    instance = DagsterInstance.ephemeral()

    tick_dt = pendulum.now()

    context = SchedulingExecutionContext.create(
        instance=instance,
        repository_def=defs.get_repository_def(),
        effective_dt=tick_dt,
        last_event_id=None,
    )

    ag_view = context.asset_graph_view

    starting_slice = context.slice_factory.from_partition_keys(down_numbers.key, {"1"})
    up_letters_slice = starting_slice.parent_asset_slice(up_letters.key)
    assert up_letters_slice.asset_key == up_letters.key
    assert up_letters_slice.materialize_partition_keys() == {"A"}

    slice_factory = context.asset_graph_view.slice_factory

    upward_from_down_1 = ag_view.create_upstream_partition_space(
        slice_factory.from_partition_keys(down_numbers.key, {"1"})
    )
    assert set(
        upward_from_down_1.asset_graph_subset.partitions_subsets_by_asset_key[
            up_letters.key
        ].get_partition_keys()
    ) == {"A"}

    assert upward_from_down_1.get_asset_slice(up_letters.key).materialize_asset_partitions() == {
        AssetPartition(up_letters.key, "A")
    }

    assert upward_from_down_1.root_asset_keys == {up_letters.key}
    assert upward_from_down_1.toposort_asset_levels == [{up_letters.key}, {down_numbers.key}]
    assert upward_from_down_1.toposort_asset_keys == [up_letters.key, down_numbers.key]

    upward_from_up_a = ag_view.create_upstream_partition_space(
        slice_factory.from_partition_keys(up_letters.key, {"A"})
    )

    assert upward_from_up_a.root_asset_keys == {up_letters.key}
    assert upward_from_up_a.toposort_asset_keys == [up_letters.key]


def test_two_assets_always_include() -> None:
    @asset(scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def up() -> None:
        ...

    @asset(deps=[up], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def down() -> None:
        ...

    defs = Definitions([up, down])

    assert materialize([up, down]).success

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)
    slice_factory = context.asset_graph_view.slice_factory
    plan = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[slice_factory.unpartitioned(down.key)],
    )

    assert plan.launch_partition_space.get_asset_slice(up.key).is_nonempty


def test_three_assets_one_root_always_include_diamond() -> None:
    @asset(scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def up() -> None:
        ...

    @asset(deps=[up], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def down1() -> None:
        ...

    @asset(deps=[up], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def down2() -> None:
        ...

    defs = Definitions([up, down1, down2])

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)
    slice_factory = context.asset_graph_view.slice_factory

    plan = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[slice_factory.unpartitioned(down1.key)],
    )

    assert plan.launch_partition_space.asset_keys == {up.key, down2.key, down1.key}

    assert plan.launch_partition_space.get_asset_slice(up.key).is_nonempty


def test_three_assets_one_root_one_excludes_diamond() -> None:
    @asset(scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def up() -> None:
        ...

    @asset(deps=[up], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def down1() -> None:
        ...

    @asset(deps=[up], scheduling_policy=NeverIncludeSchedulingPolicy())
    def down2() -> None:
        ...

    defs = Definitions([up, down1, down2])

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)

    plan = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[context.slice_factory.unpartitioned(down1.key)],
    )

    # down2 should not be included in the launch
    assert plan.launch_partition_space.asset_keys == {up.key, down1.key}

    assert plan.launch_partition_space.get_asset_slice(up.key).is_nonempty


def partition_keys(plan: ReactiveSchedulingPlan, asset_key: AssetKey) -> Set[PartitionKey]:
    return set(plan.launch_partition_space.get_asset_slice(asset_key).materialize_partition_keys())


def test_basic_partition_launch() -> None:
    letters_static_partition_def = StaticPartitionsDefinition(["A", "B", "C"])
    numbers_static_partition_def = StaticPartitionsDefinition(["1", "2", "3"])

    @asset(
        partitions_def=letters_static_partition_def,
        scheduling_policy=AlwaysIncludeSchedulingPolicy(),
    )
    def up_letters() -> None:
        ...

    letter_to_number_mapping = StaticPartitionMapping({"A": "1", "B": "2", "C": "3"})

    @asset(
        deps=[AssetDep(up_letters, partition_mapping=letter_to_number_mapping)],
        partitions_def=numbers_static_partition_def,
        scheduling_policy=AlwaysIncludeSchedulingPolicy(),
    )
    def down_numbers() -> None:
        ...

    defs = Definitions([up_letters, down_numbers])

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)

    plan_from_down_2 = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[context.slice_factory.from_partition_keys(down_numbers.key, {"2"})],
    )

    assert partition_keys(plan_from_down_2, up_letters.key) == {"B"}

    plan_from_down_3 = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[context.slice_factory.from_partition_keys(down_numbers.key, {"3"})],
    )

    assert partition_keys(plan_from_down_3, up_letters.key) == {"C"}


def test_time_windowing_partition() -> None:
    start = pendulum.datetime(2021, 1, 1)
    end = pendulum.datetime(2021, 1, 2)
    daily_partitions_def = DailyPartitionsDefinition(start_date=start, end_date=end)
    hourly_partitions_def = HourlyPartitionsDefinition(start_date=start, end_date=end)

    @asset(partitions_def=hourly_partitions_def, scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def up_hourly() -> None:
        ...

    @asset(
        deps=[up_hourly],
        partitions_def=daily_partitions_def,
        scheduling_policy=AlwaysIncludeSchedulingPolicy(),
    )
    def down_daily() -> None:
        ...

    defs = Definitions([up_hourly, down_daily])

    instance = DagsterInstance.ephemeral()

    context = build_test_context(defs, instance)

    plan = build_reactive_scheduling_plan(
        context=context,
        starting_slices=[
            context.slice_factory.from_time_window(up_hourly.key, TimeWindow(start, end))
        ],
    )

    assert (
        plan.launch_partition_space.get_asset_slice(down_daily.key).materialize_asset_partitions()
        == context.slice_factory.from_time_window(
            down_daily.key, TimeWindow(start, end)
        ).materialize_asset_partitions()
    )


def test_on_any_parent_updated() -> None:
    @asset
    def upup() -> None:
        ...

    @asset(deps=[upup], scheduling_policy=OnAnyNewParentUpdated())
    def up() -> None:
        ...

    @asset(deps=[up], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def down() -> None:
        ...

    defs = Definitions([upup, up, down])

    instance = DagsterInstance.ephemeral()

    context_one = build_test_context(defs, instance)
    down_subset = context_one.slice_factory.unpartitioned(down.key)
    up_subset = context_one.slice_factory.unpartitioned(up.key)
    upup_subset = context_one.slice_factory.unpartitioned(upup.key)

    assert materialize([up], instance=instance).success

    assert RulesLogic.any_parent_updated(context_one, down_subset).is_nonempty
    assert RulesLogic.any_parent_updated(context_one, up_subset).is_empty
    assert RulesLogic.any_parent_updated(context_one, upup_subset).is_empty

    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_one, down_subset).asset_slice, down_subset
    )
    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_one, up_subset).asset_slice,
        context_one.empty_slice(up.key),
    )
    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_one, upup_subset).asset_slice,
        context_one.empty_slice(upup.key),
    )

    plan_one = build_reactive_scheduling_plan(
        context=context_one,
        starting_slices=[down_subset],
    )

    assert plan_one.launch_partition_space.get_asset_slice(down.key).is_nonempty
    assert plan_one.launch_partition_space.get_asset_slice(up.key).is_empty
    assert plan_one.launch_partition_space.get_asset_slice(upup.key).is_empty

    assert materialize([upup], instance=instance).success

    # with upup updated, up should be return true for any parent updated and should be included in launch plan

    context_two = build_test_context(defs, instance)

    down_subset = context_two.slice_factory.unpartitioned(down.key)
    up_subset = context_two.slice_factory.unpartitioned(up.key)
    upup_subset = context_two.slice_factory.unpartitioned(upup.key)

    assert RulesLogic.any_parent_updated(context_two, down_subset).is_nonempty
    assert RulesLogic.any_parent_updated(context_two, up_subset).is_nonempty
    assert RulesLogic.any_parent_updated(context_two, upup_subset).is_empty

    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_two, down_subset).asset_slice, down_subset
    )
    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_two, up_subset).asset_slice, up_subset
    )
    assert slices_equal(
        OnAnyNewParentUpdated().evaluate(context_two, upup_subset).asset_slice,
        context_two.empty_slice(upup.key),
    )

    plan_two = build_reactive_scheduling_plan(
        context=context_two,
        starting_slices=[down_subset],
    )

    assert plan_two.launch_partition_space.get_asset_slice(down.key).is_nonempty
    assert plan_two.launch_partition_space.get_asset_slice(up.key).is_nonempty
    assert plan_two.launch_partition_space.get_asset_slice(upup.key).is_empty


def test_unsynced() -> None:
    @observable_source_asset
    def observable_one() -> DataVersion:
        return DataVersion(str(uuid4()))
        ...

    @observable_source_asset
    def observable_two() -> DataVersion:
        return DataVersion(str(uuid4()))
        ...

    @asset(deps=[observable_one])
    def asset_one() -> None:
        ...

    @asset(deps=[observable_two])
    def asset_two() -> None:
        ...

    @asset(deps=[asset_one, asset_two], scheduling_policy=AlwaysIncludeSchedulingPolicy())
    def downstream() -> None:
        ...

    defs = Definitions([observable_one, observable_two, asset_one, asset_two, downstream])

    instance = DagsterInstance.ephemeral()

    assert observe([observable_one, observable_two], instance=instance).success
    assert materialize([asset_one, asset_two, downstream], instance=instance).success
    # all observed, then materialized. Should be synced

    context_t0 = build_test_context(defs, instance)

    downstream_subset = context_t0.slice_factory.unpartitioned(downstream.key)
    assert RulesLogic.unsynced(context_t0, downstream_subset).is_empty

    assert observe([observable_one], instance=instance).success

    # # one observed. asset_one out of sync. any but not all out of sync
    context_t1 = build_test_context(defs, instance)
    downstream_subset = context_t1.slice_factory.unpartitioned(downstream.key)
    assert RulesLogic.unsynced(context_t1, downstream_subset).is_nonempty


def test_any_all_parents_updated_partitioned() -> None:
    down_static_partitions_def = StaticPartitionsDefinition(["A", "B"])
    up_static_partitions_def = StaticPartitionsDefinition(["A1", "A2", "B1", "B2"])
    static_partitions_mapping = StaticPartitionMapping({"A1": "A", "A2": "A", "B1": "B", "B2": "B"})

    # down --> up
    # A --> A1, A2
    # B --> B1, B2

    @asset(partitions_def=up_static_partitions_def)
    def up() -> None:
        ...

    @asset(
        partitions_def=down_static_partitions_def,
        deps=[AssetDep(up, partition_mapping=static_partitions_mapping)],
    )
    def down() -> None:
        ...

    @asset(deps=[down], partitions_def=down_static_partitions_def)
    def downdown() -> None:
        ...

    defs = Definitions([up, down, downdown])

    instance = DagsterInstance.ephemeral()

    # init with 1 materialization each
    assert materialize([up], instance=instance, partition_key="A1").success
    assert materialize([up], instance=instance, partition_key="A2").success
    assert materialize([up], instance=instance, partition_key="B1").success
    assert materialize([up], instance=instance, partition_key="B2").success
    assert materialize([down], instance=instance, partition_key="A").success
    assert materialize([down], instance=instance, partition_key="B").success
    assert materialize([downdown], instance=instance, partition_key="A").success
    assert materialize([downdown], instance=instance, partition_key="B").success

    context_t0 = build_test_context(defs, instance)

    assert (
        RulesLogic.all_parents_updated(
            context_t0, context_t0.slice_factory.from_partition_keys(down.key, {"A", "B"})
        ).materialize_partition_keys()
        == set()
    )

    assert (
        RulesLogic.any_parent_updated(
            context_t0, context_t0.slice_factory.from_partition_keys(down.key, {"A", "B"})
        ).materialize_partition_keys()
        == set()
    )

    # now only touch up.A1
    assert materialize([up], instance=instance, partition_key="A1").success

    context_t1 = build_test_context(defs, instance)
    # only A1 (not A2) updated so this empty
    assert (
        RulesLogic.all_parents_updated(
            context_t1, context_t1.slice_factory.from_partition_keys(down.key, {"A", "B"})
        ).materialize_partition_keys()
        == set()
    )

    # only A1 means that A has at least one parent updated but B does not
    assert RulesLogic.any_parent_updated(
        context_t1, context_t1.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == {"A"}

    # up is totally in sync
    assert (
        RulesLogic.unsynced(
            context_t1, context_t1.slice_factory.complete_asset_slice(up.key)
        ).materialize_partition_keys()
        == set()
    )

    # down.A unsynced
    assert RulesLogic.unsynced(
        context_t1, context_t1.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == set("A")

    # downdown.A unsynced trasitively
    assert RulesLogic.unsynced(
        context_t1, context_t1.slice_factory.from_partition_keys(downdown.key, {"A", "B"})
    ).materialize_partition_keys() == set("A")

    # now touch up.A2
    assert materialize([up], instance=instance, partition_key="A2").success

    context_t2 = build_test_context(defs, instance)

    # A1 and A2 updated so A has all parents updated
    assert RulesLogic.all_parents_updated(
        context_t2, context_t2.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == {"A"}

    # A1 and A2 updated so A has any parents updated
    assert RulesLogic.any_parent_updated(
        context_t2, context_t2.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == {"A"}

    # now touch up.B1
    assert materialize([up], instance=instance, partition_key="B1").success

    context_t3 = build_test_context(defs, instance)

    # A1 and A2 updated so A has all parents updated. Only B1 so B not included
    assert RulesLogic.all_parents_updated(
        context_t3, context_t3.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == {"A"}

    # A1 and A2 updated so A has all parents updated. Only B1 but B included because any
    assert RulesLogic.any_parent_updated(
        context_t3, context_t3.slice_factory.from_partition_keys(down.key, {"A", "B"})
    ).materialize_partition_keys() == {"A", "B"}


def test_default_scheduling_policy() -> None:
    # year's worth of daily partitions
    start = pendulum.datetime(2021, 1, 1)
    end = pendulum.datetime(2022, 1, 1)
    daily_partitions_def = DailyPartitionsDefinition(start_date=start, end_date=end)

    @asset(partitions_def=daily_partitions_def, scheduling_policy=DefaultSchedulingPolicy())
    def root() -> None:
        ...

    @asset(
        deps=[root],
        scheduling_policy=DefaultSchedulingPolicy(),
        partitions_def=daily_partitions_def,
    )
    def up(context) -> None:
        ...

    @asset(
        deps=[up],
        scheduling_policy=DefaultSchedulingPolicy(),
        partitions_def=daily_partitions_def,
    )
    def an_asset(context: AssetExecutionContext) -> None:
        ...

    defs = Definitions([root, up, an_asset])

    instance = DagsterInstance.ephemeral()

    # init graph. materialize last partitionn on all. then make root updated.
    minute_before_end = end - pendulum.duration(minutes=1)
    partition_key = daily_partitions_def.partition_key_for_dt(minute_before_end)
    assert materialize([root, up, an_asset], instance=instance, partition_key=partition_key).success
    assert materialize([root], instance=instance, partition_key=partition_key).success

    context_t0 = build_test_context(defs, instance)

    last_100_days = TimeWindow(end - pendulum.duration(days=100), end)
    an_asset_last_100 = context_t0.slice_factory.from_time_window(
        asset_key=an_asset.key,
        time_window=last_100_days,
    )

    plan_t0 = build_reactive_scheduling_plan(context_t0, starting_slices=[an_asset_last_100])
    # root is in sync so no launch
    assert (
        plan_t0.launch_partition_space.get_asset_slice(root.key).materialize_partition_keys()
        == set()
    )
    # rest are filtered to latest time window
    assert plan_t0.launch_partition_space.get_asset_slice(up.key).materialize_partition_keys() == {
        partition_key
    }
    assert plan_t0.launch_partition_space.get_asset_slice(
        an_asset.key
    ).materialize_partition_keys() == {partition_key}


def test_disjointed_unsynced() -> None:
    # Right now the algorithm will break if is a step where a check like unsynced or parent
    # updated ends up producing a disjointed, time-partitioned asset slice
    ...

    jan_one_start = pendulum.datetime(2024, 1, 1)
    jan_two_start = pendulum.datetime(2024, 1, 2)
    jan_three_start = pendulum.datetime(2024, 1, 3)
    jan_four_start = pendulum.datetime(2024, 1, 4)

    daily_partitions_def = DailyPartitionsDefinition(
        start_date=jan_one_start, end_date=jan_four_start
    )

    def _to_key(dt: pendulum.DateTime) -> str:
        return daily_partitions_def.partition_key_for_dt(dt)

    def _to_key_set(*dts: pendulum.DateTime) -> Set[str]:
        return {_to_key(dt) for dt in dts}

    @asset(
        partitions_def=daily_partitions_def,
        scheduling_policy=SchedulingPolicy.for_expr(Expr.unsynced()),
    )
    def up() -> None:
        ...

    @asset(
        deps=[up],
        partitions_def=daily_partitions_def,
        scheduling_policy=SchedulingPolicy.for_expr(Expr.unsynced()),
    )
    def down() -> None:
        ...

    defs = Definitions([up, down])

    instance = DagsterInstance.ephemeral()

    context_t0 = build_test_context(defs, instance)

    all_partition_keys = {
        _to_key(jan_one_start),
        _to_key(jan_two_start),
        _to_key(jan_three_start),
        # "2024-01-01",
        # "2024-01-02",
        # "2024-01-03",
    }

    # confirm that we understand edge behavior of time partitioning
    assert (
        context_t0.slice_factory.complete_asset_slice(up.key).materialize_partition_keys()
        == all_partition_keys
    )

    plan_t0 = build_reactive_scheduling_plan(
        context=context_t0,
        starting_slices=[context_t0.asset_graph_view.slice_factory.complete_asset_slice(down.key)],
    )

    assert plan_t0
    # all unsynced to there is a launch space
    assert not plan_t0.launch_partition_space.is_empty

    # materialize all

    for partition_key in all_partition_keys:
        assert materialize([up, down], partition_key=partition_key, instance=instance).success

    context_t1 = build_test_context(defs, instance)

    plan_t1 = build_reactive_scheduling_plan(
        context=context_t1,
        starting_slices=[context_t1.asset_graph_view.slice_factory.complete_asset_slice(down.key)],
    )

    assert context_t1.asset_graph_view.get_asset_slice(up.key).compute_unsynced().is_empty
    assert context_t1.asset_graph_view.get_asset_slice(down.key).compute_unsynced().is_empty

    assert plan_t1
    assert plan_t1.launch_partition_space.is_empty

    # now materialize disjoint partitions (jan one and jan three)

    assert materialize(
        [up],
        instance=instance,
        partition_key=_to_key(jan_one_start),
    ).success

    assert materialize(
        [up],
        instance=instance,
        partition_key=_to_key(jan_three_start),
    ).success

    context_t2 = build_test_context(defs, instance)

    plan_t2 = build_reactive_scheduling_plan(
        context=context_t2,
        starting_slices=[context_t2.asset_graph_view.slice_factory.complete_asset_slice(down.key)],
    )

    assert plan_t2
    assert not plan_t2.launch_partition_space.is_empty

    assert plan_t2.launch_partition_space.get_asset_slice(
        up.key
    ).is_empty  # all up partitions are in sync

    assert plan_t2.launch_partition_space.get_asset_slice(
        down.key
    ).materialize_partition_keys() == _to_key_set(jan_one_start, jan_three_start)