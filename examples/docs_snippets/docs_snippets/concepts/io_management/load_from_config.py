from dagster import OpExecutionContext, op, job


# def_start_marker
@op
def my_op(context: OpExecutionContext, input_string: str):
    context.log.info(f"input string: {input_string}")


@job
def my_job():
    my_op()


# def_end_marker


def execute_with_config():
    # execute_start_marker
    my_job.execute_in_process(
        run_config={"ops": {"my_op": {"inputs": {"input_string": {"value": "marmot"}}}}}
    )
    # execute_end_marker
