# Inlined from /metadata-ingestion/examples/library/dataset_schema.py
# Imports for urn construction utility methods
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DateTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

import time

from pydantic import BaseModel
from typing import List, Dict, Union, Any, Optional, Tuple, Iterable

class neo4jDataset(BaseModel):
    name: str
    description: str
    platform: str = "neo4j"
    env: str = "PROD"
    fields: List[Dict[str, str]]

current_timestamp = AuditStampClass(
     #current time
    time=int(time.time()),
    actor="urn:li:corpuser:datahub",
    message="Data Quality Incident"
) 

source_dataset = neo4jDataset(
    name="customer",
    description="Customer Information",
    fields=[
        {
            "fieldPath": "name",
            "type": "string",
            "description": "Name of the customer",
        },
        {
            "fieldPath": "age",
            "type": "int",
            "description": "Age of the customer",
        },
    ]
)

fields = [ SchemaFieldClass(
            fieldPath=field["fieldPath"],
            type=SchemaFieldDataTypeClass(
                type=StringTypeClass() if field["type"] == "string" else DateTypeClass()
            ),
            nativeDataType=field["type"],
            description=field["description"],
        ) for field in source_dataset.fields]


event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="neo4j", name="default.customer", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="customer",  # not used
        platform=make_data_platform_urn("neo4j"),  # important <- platform must be an urn
        version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
        hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
        platformSchema=OtherSchemaClass(rawSchema="__insert raw schema here__"),
        lastModified=current_timestamp,
        fields=fields,
    ),
)

print(event)

# Create rest emitter
# rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
# rest_emitter.emit(event)
