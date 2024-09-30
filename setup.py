from setuptools import find_packages, setup

setup(
    name="livedocs",
    version="0.1",
    packages=find_packages(exclude=["tests"]),
    description="Python SDK for Livedocs virtual environment",
    author="Livedocs",
    install_requires=[
        "requests",
        "jinja2",
        "polars",
        "vegafusion[embed]",
        "pandas",
        "pyarrow",
        "db-dtypes",
        "duckdb",
        "altair",
        "pydantic",
        "sentry-sdk"
    ],
    python_requires=">=3.12",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
