import json
import pytest
from datahub.ingestion.extractor import schema_util

from kiota_abstractions.authentication.anonymous_authentication_provider import (
    AnonymousAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from apicurioregistrysdk.client.groups.item.artifacts.artifacts_request_builder import (
    ArtifactsRequestBuilder,
)
from apicurioregistrysdk.client.models.artifact_meta_data import ArtifactMetaData
from apicurioregistrysdk.client.registry_client import RegistryClient
from apicurioregistrysdk.client.models.artifact_content import ArtifactContent

REGISTRY_HOST = "localhost"
REGISTRY_PORT = 8085

@pytest.mark.asyncio
async def test_upload_download():
    auth_provider = AnonymousAuthenticationProvider()
    request_adapter = HttpxRequestAdapter(auth_provider)
    request_adapter.base_url = f"http://{REGISTRY_HOST}:{REGISTRY_PORT}/apis/registry/v2"

    registry_client = RegistryClient(request_adapter)
    payload = ArtifactContent()
    payload.content = """{
        "openapi": "3.0.0",
        "info": {
            "title": "My API",
            "version": "1.0.0"
        },
        "paths": {}
    }"""
    # meta_data = await registry_client.groups.by_group_id("default").artifacts.post(payload)
    # assert meta_data.id is not None

    artifactid = "rep-avd-track-avro-v1-value"
    grp = ""

    return_search = await registry_client.search.artifacts.get()
    for artifact in return_search.artifacts:
        if artifact.id == artifactid:
            grp = artifact.group_id

    return_artifact = await (registry_client.groups.by_group_id(grp)
                             .artifacts.by_artifact_id(artifactid)
                             .get())
    return_str = str(return_artifact, "utf-8")
    #print(return_str)

    cleaned_str = json.dumps(json.loads(return_str), separators=(",", ":"))

    fields = schema_util.avro_schema_to_mce_fields(cleaned_str, False, False, False)
    print(fields)
    # assert json.loads(return_artifact) == json.loads(payload.content)
