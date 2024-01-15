from setuptools import setup, find_packages

setup(
    name="sterunets",
    version="0.1.0",
    description="",
    author="GHHag",
    url="https://github.com/GHHag/sterunets/nets",
    packages=find_packages(),
    install_requires=[
        "grpcio",
        "numpy",
        "pandas",
    ],
)
