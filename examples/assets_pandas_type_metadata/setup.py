from setuptools import setup, find_packages

setup(
    name="assets_pandas_type_metadata",
    packages=find_packages(exclude=["assets_pandas_type_metadata_tests"]),
    install_requires=[
        "dagster",
        "dagster-pandera",
        "jupyterlab",
        "matplotlib",
        "seaborn",
        "pandera",
        "pandas",
        "pyarrow",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
