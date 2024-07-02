from dagster._core.libraries import DagsterLibraryRegistry

from .job import (
    DagsterK8sJobConfig as DagsterK8sJobConfig,
    K8sConfigMergeBehavior as K8sConfigMergeBehavior,
    construct_dagster_k8s_job as construct_dagster_k8s_job,
)
from .ops import (
    k8s_job_op as k8s_job_op,
    execute_k8s_job as execute_k8s_job,
)
from .pipes import (
    PipesK8sClient as PipesK8sClient,
    PipesK8sPodLogsMessageReader as PipesK8sPodLogsMessageReader,
)
from .version import __version__ as __version__
from .executor import k8s_job_executor as k8s_job_executor
from .launcher import K8sRunLauncher as K8sRunLauncher

DagsterLibraryRegistry.register("dagster-k8s", __version__)
