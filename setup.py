from setuptools import find_packages, setup

setup_output = setup(
    name="apicurio_datahub_kafka",
    version="2.0.1",
    description="DataHub ApiCurio Schema Registry for Kafka Source",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["acryl-datahub[kafka]"],
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)
