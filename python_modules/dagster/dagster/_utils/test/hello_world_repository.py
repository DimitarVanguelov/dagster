from dagster._core.definitions import op, job, repository


@op
def hello_world(_):
    pass


@job
def hello_world_job():
    hello_world()


@repository
def hello_world_repository():
    return [hello_world_job]
