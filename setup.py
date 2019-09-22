import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="yasmss",
    version="0.0.1",
    author="Ashirwad Pradhan",
    author_email="ashirwad.pradhan007@gmail.com",
    description=("A REST client to run SQL query into  "
                 "Map Reduce and Spark jobs"),
    license="MIT",
    keywords="hadoop map-reduce spark json rest-api REST ",
    url="https://github.com/AshirwadPradhan/yasmss",
    packages=['yasmss', 'tests'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Distributed Computing",
        "License :: MIT License",
    ],
)
