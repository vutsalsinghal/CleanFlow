# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

try:
    long_description = open("README.rst").read()
except IOError:
    long_description = ""

setup(
    name="CleanFlow",
    version="0.1.0",
    description="A a framework for cleaning, pre-processing and exploring data in a scalable and distributed manner.",
    license="MIT",
    author="Vutsal Singhal",
    author_email="vutsalsinghal@nyu.edu",
    url = 'https://github.com/vutsalsinghal/CleanFlow',
    download_url = 'https://github.com/vutsalsinghal/CleanFlow/archive/0.1.0.tar.gz',
    packages=find_packages(),
    install_requires=['pyspark'],
    long_description=long_description,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.4",
    ]
)
