import os

import yaml
import pytest
from dagster import job
from dagster_spark import spark_resource, create_spark_op

CONFIG = """
ops:
  first_pi:
    config:
      master_url: "local[2]"
      deploy_mode: "client"
      spark_conf:
        spark:
          app:
            name: "first_pi"
      application_jar: {jar_path}
      application_arguments: '10'

  second_pi:
    config:
      master_url: "local[2]"
      deploy_mode: "client"
      spark_conf:
        spark:
          app:
            name: "second_pi"
      application_jar: {jar_path}
      application_arguments: '10'

  third_pi:
    config:
      master_url: "local[2]"
      deploy_mode: "client"
      spark_conf:
        spark:
          app:
            name: "third_pi"
      application_jar: {jar_path}
      application_arguments: '10'
"""


@pytest.mark.skip("for local testing only, we don't have $SPARK_HOME on buildkite yet")
def test_multiple_spark_jobs():
    @job(resource_defs={"spark": spark_resource})
    def job_def():
        for op_name in ["first_pi", "second_pi", "third_pi"]:
            create_spark_op(op_name, main_class="org.apache.spark.examples.SparkPi")()

    # Find SPARK_HOME to get to spark examples jar
    base_path = os.path.expandvars("${SPARK_HOME}/examples/jars/")
    jar_path = None
    for fname in os.listdir(base_path):
        if fname.startswith("spark-examples"):
            jar_path = os.path.join(base_path, fname)

    result = job_def.execute_in_process(yaml.safe_load(CONFIG.format(jar_path=jar_path)))
    assert result.success


@pytest.mark.skip("for local testing only, we don't have $SPARK_HOME on buildkite yet")
def test_multiple_spark_job_jobs():
    @job(resource_defs={"spark": spark_resource})
    def my_job():
        for op_name in ["first_pi", "second_pi", "third_pi"]:
            create_spark_op(op_name, main_class="org.apache.spark.examples.SparkPi")()

    # Find SPARK_HOME to get to spark examples jar
    base_path = os.path.expandvars("${SPARK_HOME}/examples/jars/")
    jar_path = None
    for fname in os.listdir(base_path):
        if fname.startswith("spark-examples"):
            jar_path = os.path.join(base_path, fname)

    result = my_job.execute_in_process(run_config=yaml.safe_load(CONFIG.format(jar_path=jar_path)))
    assert result.success
