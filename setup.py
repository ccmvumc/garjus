from setuptools import setup, find_packages

setup(
    name="garjus",
    version="1.0.0",
    author="Brian D. Boyd",
    author_email="brian.d.boyd@vumc.org",
    description="Python package for managing imaging data in REDCap and XNAT",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/bud42/garjus",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    install_requires=[
        "pandas",
        "pycap",
        "pyxnat",
        "dax",
        "click",
        "sphinx",
        "pydot",
        "plotly",
    ],
    entry_points={"console_scripts": ["garjus = garjus.cli:cli"]},
)
