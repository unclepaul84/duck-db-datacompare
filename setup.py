from setuptools import setup, find_packages
import os


with open("readme.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("version.txt") as f:
    version = f.read().strip()

setup(
    name="delta-lens",
    version=version,
    author="Paul",
    author_email="unclepaul84@gmail.com",
    description="A tool for comparing large datasets using DuckDB",
    url="https://github.com/unclepaul84/duck-db-datacompare",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    package_data={
        'delta_lens': ['config_schema.json'],
        '': ['version.txt'],
        '': ['readme.md']
    },
    
    include_package_data=True,

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
    ],
    python_requires=">=3.9",
    install_requires=[
        "duckdb>=1.2.0",
        "pandas>=2.2.3",
        "numpy>=2.2.2",
        "jsonschema>=4.23.0"
    ],
    entry_points={
        'console_scripts': [
            'deltalens=delta_lens.cli:main',
        ],
    },
)