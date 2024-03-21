from setuptools import find_packages, setup

setup_output = setup(
    name="apicurio_datahub_kafka",
    version="1.1.0",
    description="ApiCurio Schema Registry for Kafka Source",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub[kafka]"],
)
