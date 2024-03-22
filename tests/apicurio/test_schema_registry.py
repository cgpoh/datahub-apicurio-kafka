import os
import time

import pytest
import requests

from datahub.ingestion.source.kafka import KafkaSourceConfig, KafkaSourceReport

from kiota_abstractions.authentication.anonymous_authentication_provider import (
    AnonymousAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from apicurioregistrysdk.client.groups.item.artifacts.artifacts_request_builder import (
    ArtifactsRequestBuilder,
)
from apicurioregistrysdk.client.registry_client import RegistryClient
from apicurioregistrysdk.client.models.artifact_content import ArtifactContent

from testcontainers.core.container import DockerContainer
from apicurio_kafka.apicurio_schema_registry import ApicurioSchemaRegistry

REGISTRY_HOST = "REGISTRY_HOST"
REGISTRY_PORT = "REGISTRY_PORT"
MAX_POLL_TIME = 120
POLL_INTERVAL = 1
start_time = time.time()

apicurio_container = DockerContainer("apicurio/apicurio-registry-mem:2.4.4.Final").with_exposed_ports(8080)

def poll_for_ready():
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= MAX_POLL_TIME:
            print("Polling timed out.")
            break

        print("Attempt to connect")
        try:
            response = requests.get(f"http://{os.environ[REGISTRY_HOST]}:{os.environ[REGISTRY_PORT]}/apis/registry/v2")
            if response.status_code == 200:
                print("Server is up!")
                break
        except requests.exceptions.ConnectionError:
            pass

        # Wait for the specified poll interval before trying again
        time.sleep(POLL_INTERVAL)

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    apicurio_container.start()

    def remove_container():
        apicurio_container.stop()

    request.addfinalizer(remove_container)
    os.environ[REGISTRY_HOST] = apicurio_container.get_container_host_ip()
    os.environ[REGISTRY_PORT] = apicurio_container.get_exposed_port(8080)
    poll_for_ready()

@pytest.mark.asyncio
async def test_apicurio_schema_registry():
    auth_provider = AnonymousAuthenticationProvider()
    request_adapter = HttpxRequestAdapter(auth_provider)
    request_adapter.base_url = f"http://{os.environ[REGISTRY_HOST]}:{os.environ[REGISTRY_PORT]}/apis/registry/v2"

    registry_client = RegistryClient(request_adapter)
    payload = ArtifactContent()
    payload.content = """{
        "type": "record",
        "namespace": "test",
        "name": "TestRecord",
        "fields": [
            {
                "name": "nametest",
                "type" : "string"
            }
        ]
    }"""
    query_params = ArtifactsRequestBuilder.ArtifactsRequestBuilderPostQueryParameters(
        canonical=True, if_exists='RETURN_OR_UPDATE'
    )

    request_configuration = (
        ArtifactsRequestBuilder.ArtifactsRequestBuilderPostRequestConfiguration(
            headers={"X-Registry-ArtifactId": "test-value"}, query_parameters=query_params
        )
    )
    meta_data = await registry_client.groups.by_group_id("foo.group").artifacts.post(payload, request_configuration=request_configuration)
    assert meta_data.id == "test-value"

    kafka_source_config = KafkaSourceConfig()
    kafka_source_config.connection.schema_registry_url = f"http://{os.environ[REGISTRY_HOST]}:{os.environ[REGISTRY_PORT]}/apis/registry/v2"

    kafka_source_report = KafkaSourceReport()

    apicurio_schema_registry = ApicurioSchemaRegistry(kafka_source_config, kafka_source_report)
    apicurio_schema_registry = apicurio_schema_registry.create(kafka_source_config, kafka_source_report)
    schema_metadata = apicurio_schema_registry.get_schema_metadata("test", "")
    assert schema_metadata is not None
