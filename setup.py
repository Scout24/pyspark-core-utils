import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyspark-core-utils",
    version="1.1.1",
    author="ImmobilienScout24",
    description="PySpark core utils library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Scout24/pyspark-core-utils",
    project_urls={
        "Bug Tracker": "https://github.com/Scout24/pyspark-core-utils/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=["importlib-resources==5.4.0",
                      "PyYAML==6.0",
                      "dotmap==1.3.26"]
)
