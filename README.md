# datahub-apicurio-kafka
ApiCurio Schema Registry implementation for [DataHub Kafka ingestion source](https://datahubproject.io/docs/generated/ingestion/sources/kafka/). Only support AVRO schema currently.

## Sample Ingestion Recipe
A sample recipe can be found [here](./kafka_src_recipe.yaml)

## Config Details
If `schema_registry_config.pagination` is omitted, all records from Schema Registry will be fetched.
To limit number of records fetched from the Schema Registry, you can set the `schema_registry_config.pagination` as shown below:
<pre>
source:
  type: "kafka"
  config:
    platform_instance: "local-docker"
    schema_registry_class: "apicurio_kafka.apicurio_schema_registry.ApicurioSchemaRegistry"
    connection:
      bootstrap: "localhost:9093"
      schema_registry_url: http://localhost:8085/apis/registry/v2
      <b>schema_registry_config:
        pagination: 100</b>
</pre>

## Installation
Package can be installed using `pip install apicurio-datahub-kafka`
