from setuptools import setup

setup(
    name="livedocs-lib",
    version="0.1",
    packages=["livedocs"],
    description="Ingestor cloud function library",
    author="Livedocs",
    install_requires=[
        "functions-framework==3.5.0",
        "google-auth==2.29.0",
        "google-cloud-pubsub==2.21.0",
        "requests==2.31.0",
    ],
    python_requires=">=3.12",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)