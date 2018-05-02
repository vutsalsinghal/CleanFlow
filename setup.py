# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

try:
    long_description = open("README.md").read()
except IOError:
    long_description = ""

setup(
    name="CleanFlow",
    version="1.2.0a1",
    description="A a framework for cleaning, pre-processing and exploring data in a scalable and distributed manner.",
    license="MIT",
    author="Vutsal Singhal",
    author_email="vutsalsinghal@nyu.edu",
    url = 'https://github.com/vutsalsinghal/CleanFlow',
    download_url = 'https://github.com/vutsalsinghal/CleanFlow/archive/master.zip',
    packages=find_packages(),
    install_requires=['pyspark'],
    long_description=long_description,
    classifiers=[
    	"Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ]
)
