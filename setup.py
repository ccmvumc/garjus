from setuptools import setup, find_packages

setup(
    name="garjus",
    version="0.1.0",
    author="Brian D. Boyd",
    author_email="brian.d.boyd@vumc.org",
    description="A Python package for managing data and images in REDCap and XNAT",
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
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
        "requests",
        "pycap",
        "pyxnat",
        "dax",
        "click",
        "sphinx",
        "yapf",
        "twine",
    ],
    entry_points={"console_scripts": ["garjus = garjus.cli:cli"]},
)
