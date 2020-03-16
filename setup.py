import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="csv2parquet",
    version="1.0.0",
    author="Alonso-Roman",
    author_email="alonso.roman@outlook.com",
    description="A csv to parquet converter for Green Flag",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://none",
    packages=setuptools.find_packages(),
    # Include additional files into the package
    include_package_data=True,

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6.5',

    # Dependent packages (distributions)
    install_requires=[
        "pytest",
        "configparser",
        "pyspark",
        "pyspark-stubs",
        "pandas"
    ],
)