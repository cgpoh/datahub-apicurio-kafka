import asyncio
import json
import logging

import nest_asyncio
nest_asyncio.apply()

from hashlib import md5
from typing import List, Optional, Tuple

from apicurioregistrysdk.client.models.searched_artifact import SearchedArtifact
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from kiota_abstractions.authentication.anonymous_authentication_provider import (
    AnonymousAuthenticationProvider,
)
from apicurioregistrysdk.client.registry_client import RegistryClient
from apicurioregistrysdk.client.models.artifact_search_results import ArtifactSearchResults

from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.kafka import KafkaSourceConfig, KafkaSourceReport
from datahub.ingestion.source.kafka_schema_registry_base import KafkaSchemaRegistryBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata, SchemaField, KafkaSchema

logger = logging.getLogger(__name__)


class ApicurioSchemaRegistry(KafkaSchemaRegistryBase):
    """
    This is Apicurio schema registry specific implementation of datahub.ingestion.source.kafka import SchemaRegistry
    It knows how to get SchemaMetadata of a topic from ApicurioSchemaRegistry
    """

    def __init__(
        self, source_config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> None:
        self.loop = None
        self.source_config: KafkaSourceConfig = source_config
        self.report: KafkaSourceReport = report

    async def _async_init(
        self
    ) -> None:
        auth_provider = AnonymousAuthenticationProvider()
        request_adapter = HttpxRequestAdapter(auth_provider)
        request_adapter.base_url = self.source_config.connection.schema_registry_url
        self.registry_client = RegistryClient(request_adapter)

        try:
            self.known_schema_registry_subjects: ArtifactSearchResults = await self.registry_client.search.artifacts.get()
            logger.info(f"Known schema registry subjects: {self.known_schema_registry_subjects.count}")
        except Exception as e:
            logger.warning(f"Failed to get artifacts from schema registry: {e}")

    @classmethod
    def create(
        cls, source_config: KafkaSourceConfig, report: KafkaSourceReport
    ) -> "ApicurioSchemaRegistry":
        self = cls(source_config, report)
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self._async_init())

        logger.info(f"ApicurioSchemaRegistry initialised")
        return self

    def _get_artifact_for_topic(self, topic: str, is_key_schema: bool) -> Optional[SearchedArtifact]:
        subject_key_suffix: str = "-key" if is_key_schema else "-value"

        for artifact in self.known_schema_registry_subjects.artifacts:
            if (
                artifact.id.startswith(topic)
                and artifact.id.endswith(subject_key_suffix)
            ):
                return artifact
        return None

    @staticmethod
    def _compact_schema(schema_str: str) -> str:
        # Eliminate all white-spaces for a compact representation.
        return json.dumps(json.loads(schema_str), separators=(",", ":"))

    async def _get_schema_fields(
        self, topic: str, artifact: SearchedArtifact, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if artifact.type == "AVRO":
            return_artifact = await (self.registry_client.groups.by_group_id(artifact.group_id)
                                     .artifacts.by_artifact_id(artifact.id)
                                     .get())
            cleaned_str = self._compact_schema(str(return_artifact, "utf-8"))
            # "value.id" or "value.[type=string]id"
            fields = schema_util.avro_schema_to_mce_fields(cleaned_str)
        elif not self.source_config.ignore_warnings_on_schema_type:
            self.report.report_warning(
                topic,
                f"Parsing kafka schema type {artifact.type} is currently not implemented",
            )
        return fields

    async def _get_schema_and_fields(
        self, topic: str,
        is_key_schema: bool
    ) -> Tuple[Optional[SearchedArtifact], List[SchemaField]]:
        artifact: Optional[SearchedArtifact] = None
        schema_type_str: str = "key" if is_key_schema else "value"
        artifact = self._get_artifact_for_topic(
            topic=topic, is_key_schema=is_key_schema
        )
        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if artifact is not None:
            logger.debug(
                f"The {schema_type_str} schema subject:'{artifact.id}', grp ID:'{artifact.group_id}' is found for topic:'{topic}'."
            )
            fields = await self._get_schema_fields(
                topic=topic, artifact=artifact, is_key_schema=is_key_schema
            )
        else:
            logger.debug(
                f"For topic: {topic}, the schema registry subject for the schema is not found."
            )
        return artifact, fields

    async def _get_artifact(
        self, artifact: SearchedArtifact
    ) -> str:
        return_artifact = await (self.registry_client.groups.by_group_id(artifact.group_id)
                                     .artifacts.by_artifact_id(artifact.id)
                                     .get()
                                     )
        return str(return_artifact, "utf-8")

    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        logger.info(f"Inside get_schema_metadata {topic} {platform_urn}")
        # Process the value schema
        artifact, fields = self.loop.run_until_complete(
            self._get_schema_and_fields(
                topic=topic, is_key_schema=False
            ) # type: Tuple[Optional[SearchedArtifact], List[SchemaField]]
        )

        # Process the key schema
        key_artifact, key_fields = self.loop.run_until_complete(
            self._get_schema_and_fields(
                topic=topic, is_key_schema=True
            )  # type:Tuple[Optional[SearchedArtifact], List[SchemaField]]
        )

        # Create the schemaMetadata aspect.
        if artifact is not None or key_artifact is not None:
            schema = ""
            key_schema = ""

            if artifact is not None:
                schema = self.loop.run_until_complete(
                    self._get_artifact(artifact)
                )

            if key_artifact is not None:
                key_schema = self.loop.run_until_complete(
                    self._get_artifact(artifact)
                )

            # create a merged string for the combined schemas and compute an md5 hash across
            schema_as_string = (schema if artifact is not None else "") + (
                key_schema if key_artifact is not None else ""
            )
            md5_hash = md5(schema_as_string.encode()).hexdigest()

            return SchemaMetadata(
                schemaName=topic,
                version=0,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema if artifact else "",
                    documentSchemaType=artifact.type if artifact else None,
                    keySchema=key_schema if key_artifact else None,
                    keySchemaType=key_artifact.type if key_artifact else None,
                ),
                fields=key_fields + fields,
            )
        return None


