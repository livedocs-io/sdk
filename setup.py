from setuptools import find_packages, setup

setup(
    name="livedocs",
    version="0.1",
    packages=find_packages(exclude=["tests"]),
    description="Ingestor cloud function library",
    author="Livedocs",
    install_requires=[
        "google-auth==2.29.0",
        "requests==2.31.0",
        "psycopg2-binary==2.9.5",
        "jinja2==3.1.2",
        "python-dotenv",
        "google-cloud-bigquery",
        "polars==0.20.31",
        "pandas",
        "pyarrow",
        "flask",
        "db-dtypes",
    ],
    python_requires=">=3.12",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
