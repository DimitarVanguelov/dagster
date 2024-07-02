from dagster import op, job, graph, resource


def test_description_inference():
    decorators = [job, op, graph, resource]
    for decorator in decorators:

        @decorator
        def my_thing():
            """Here is some
            multiline description.
            """

        assert my_thing.description == "\n".join(["Here is some", "multiline description."])
