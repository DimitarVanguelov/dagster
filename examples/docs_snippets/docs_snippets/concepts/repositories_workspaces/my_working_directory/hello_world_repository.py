from dagster import op, job, repository


@op
def hello_world():
    pass


@job
def hello_world_pipeline():
    hello_world()


@repository
def hello_world_repository():
    return [hello_world_pipeline]
