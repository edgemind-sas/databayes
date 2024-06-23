from setuptools import setup, find_packages

# read version as __version__
exec(open("databayes/version.py").read())


setup(
    name="databayes",
    version=__version__,
    url="https://github.com/edgemind-sas/databayes",
    author="Roland Donat",
    author_email="roland.donat@gmail.com, roland.donat@edgemind.net",
    maintainer="Roland Donat",
    maintainer_email="roland.donat@edgemind.net",
    keywords="Collection of data management and data science tools",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    packages=find_packages(
        exclude=[
            "*.tests",
            "*.tests.*",
            "tests.*",
            "tests",
            "log",
            "log.*",
            "*.log",
            "*.log.*",
        ]
    ),
    description="",
    license="MIT",
    platforms="ALL",
    python_requires=">=3.9",
    install_requires=[
        "pydantic>=1.10.4",
        "pyyaml>=6.0",
        "tzlocal>=5.0.1",
        "tqdm>=4.64.1",
        "colored>=2.2.3",
        "pandas>=2.2.2",
        "pymongo>=4.4.1",
        "influxdb-client>=1.37.0",
        "mysql-connector-python>=8.3.0",
    ],
    zip_safe=False,
)
