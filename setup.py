
#!/usr/bin/env python
from setuptools import setup

setup(
    name="dbt-runner",
    version="0.1.0",
    description="Utility for running a DBT project in a cloud environment",
    author="Chris Goddard",
    url="https://dccc.org",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["dbt_run"],
    install_requires=[
        "google-cloud-bigquery>=1.20.0",
        "pyyaml==5.1.2",
        "google-cloud-pubsub==1.4.3",
        "dbt==0.16.1",
        "google-cloud-storage==1.28.1"
        # "parse=1.15.0"
    ],
    entry_points="""
    [console_scripts]
    dbt-run=dbt_run:main
    """,
    packages=["dbt_run"],
    include_package_data=True,
)
