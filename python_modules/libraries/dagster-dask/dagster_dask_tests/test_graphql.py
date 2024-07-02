from dagster._utils import file_relative_path
from dagster._cli.workspace import get_workspace_process_context_from_kwargs
from dagster._core.test_utils import instance_for_test
from dagster_graphql.test.utils import (
    infer_job_selector,
    execute_dagster_graphql,
    execute_dagster_graphql_subscription,
)
from dagster_graphql.client.query import SUBSCRIPTION_QUERY, LAUNCH_PIPELINE_EXECUTION_MUTATION


def test_execute_hammer_through_webserver():
    with instance_for_test() as instance:
        with get_workspace_process_context_from_kwargs(
            instance,
            version="",
            read_only=False,
            kwargs={
                "python_file": (file_relative_path(__file__, "hammer_job.py"),),
                "attribute": "hammer_job",
            },
        ) as workspace_process_context:
            context = workspace_process_context.create_request_context()
            selector = infer_job_selector(context, "hammer_job")

            variables = {
                "executionParams": {
                    "runConfigData": {
                        "execution": {"config": {"cluster": {"local": {}}}},
                    },
                    "selector": selector,
                    "mode": "default",
                }
            }

            start_job_result = execute_dagster_graphql(
                context,
                LAUNCH_PIPELINE_EXECUTION_MUTATION,
                variables=variables,
            )

            if start_job_result.errors:
                raise Exception(f"{start_job_result.errors}")

            run_id = start_job_result.data["launchPipelineExecution"]["run"]["runId"]

            context.instance.run_launcher.join(timeout=60)

            subscribe_results = execute_dagster_graphql_subscription(
                context, SUBSCRIPTION_QUERY, variables={"runId": run_id}
            )

            messages = [
                x["__typename"] for x in subscribe_results[0].data["pipelineRunLogs"]["messages"]
            ]

            assert "RunStartEvent" in messages
            assert "RunSuccessEvent" in messages
