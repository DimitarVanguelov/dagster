from typing import Any
from pathlib import Path

from dagster import Definitions, ResourceParam, ConfigurableIOManager, asset
from pyspark.sql import Row, DataFrame
from dagster_aws.s3 import S3Resource
from dagster_aws.emr import emr_pyspark_step_launcher
from dagster_pyspark import PySparkResource
from pyspark.sql.types import StringType, StructType, IntegerType, StructField


class ParquetIOManager(ConfigurableIOManager):
    pyspark: PySparkResource
    path_prefix: str

    def _get_path(self, context) -> str:
        return "/".join([context.resource_config["path_prefix"], *context.asset_key.path])

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))

    def load_input(self, context):
        spark = self.pyspark.spark_session
        return spark.read.parquet(self._get_path(context.upstream_output))


@asset
def people(pyspark: PySparkResource, pyspark_step_launcher: ResourceParam[Any]) -> DataFrame:
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
    return pyspark.spark_session.createDataFrame(rows, schema)


emr_pyspark = PySparkResource(spark_config={"spark.executor.memory": "2g"})


@asset
def people_over_50(pyspark_step_launcher: ResourceParam[Any], people: DataFrame) -> DataFrame:
    return people.filter(people["age"] > 50)


defs = Definitions(
    assets=[people, people_over_50],
    resources={
        "pyspark_step_launcher": emr_pyspark_step_launcher.configured(
            {
                "cluster_id": {"env": "EMR_CLUSTER_ID"},
                "local_pipeline_package_path": str(Path(__file__).parent),
                "deploy_local_pipeline_package": True,
                "region_name": "us-west-1",
                "staging_bucket": "my_staging_bucket",
                "wait_for_logs": True,
            }
        ),
        "pyspark": emr_pyspark,
        "s3": S3Resource(),
        "io_manager": ParquetIOManager(pyspark=emr_pyspark, path_prefix="s3://my-s3-bucket"),
    },
)
