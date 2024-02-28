import time, logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

import datahub.emitter.mce_builder as builder

from datahub.emitter.mce_builder import make_dataset_urn

from typing import List, Dict, Union, Any, Optional, Tuple, Iterable

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AssertionKeyClass,
    AssertionRunEventClass,
    AssertionInfoClass,
    RowCountTotalClass,
    AssertionStdParametersClass,
    VolumeAssertionInfoClass,
    AssertionStdParameterClass,
    AssertionResultClass,
    IncidentKeyClass,
    IncidentInfoClass,
    IncidentStatusClass,
    AuditStampClass
)

from pydantic import BaseModel

from pycarlo.core import Client, Session

class MonteCarloRunConfig(BaseModel):
    api_key_id: str
    api_key_secret: str

    # Snowflake data source account
    snowflake_account: Optional[str] = None

    # Errors to be ignored, e.g. timeouts
    # ignored_errors: List[str] = field(
    #     default_factory=lambda: [
    #         "Monitor timed out",
    #     ]
    # )

class ExtractorSession(BaseModel):
    id: str
    _mcon_platform_map: Dict[str, str] = {}

#rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")

logger = logging.getLogger(__name__)

graph: DataHubGraph = get_default_graph()

current_timestamp = AuditStampClass(
     #current time
    time=int(time.time()),
    actor="urn:li:corpuser:datahub",
    message="Data Quality Incident"
) 

assets_base_url = "https://getmontecarlo.com/assets"
monitors_base_url = "https://getmontecarlo.com/monitors"

connection_type_platform_map = {
    "BIGQUERY": "BIGQUERY",
    "REDSHIFT": "REDSHIFT",
    "SNOWFLAKE": "SNOWFLAKE",
}

def _parse_monitors(self, monitors) -> None:
    for monitor in monitors["get_monitors"]:
        monitor_severity = DataMonitorSeverity.UNKNOWN
        if monitor["severity"]:
            try:
                monitor_severity = DataMonitorSeverity(
                    monitor["severity"].toUpper()
                )
            except Exception:
                logger.warning(f"Unknown severity {monitor['severity']}")

        data_monitor = DataMonitor(
            title=monitor["name"],
            description=monitor["description"],
            owner=monitor["creatorId"],
            status=self._parse_monitor_status(monitor),
            severity=monitor_severity,
            url=f"{monitors_base_url}/{monitor['uuid']}",
            last_run=parser.parse(monitor["prevExecutionTime"]),
            targets=[
                DataMonitorTarget(column=field.upper())
                for field in monitor["monitorFields"] or []
            ],
        )

        for index, entity in enumerate(monitor["entities"]):
            if index > len(monitor["entityMcons"]) - 1:
                logger.warning(f"Unmatched entity mcon in monitor {monitor}")
                break

            mcon = monitor["entityMcons"][index]
            platform = self._mcon_platform_map.get(mcon)
            if platform is None:
                logger.warning(f"Unable to determine platform for {mcon}")
                continue

            name = self._convert_dataset_name(entity)
            dataset = self._init_dataset(name, platform)
            dataset.data_quality.url = f"{assets_base_url}/{mcon}/custom-monitors"
            dataset.data_quality.monitors.append(data_monitor)

def _fetch_monitors(self, client) -> None:
    """Fetch all monitors

    See https://apidocs.getmontecarlo.com/#query-getMonitors
    """

    try:
        monitors = client(
            """
            {
                getMonitors {
                uuid
                name
                description
                entities
                entityMcons
                severity
                monitorStatus
                monitorFields
                creatorId
                prevExecutionTime
                }
            }
            """
        )

        logger.info(f"Fetched {len(monitors['get_monitors'])} monitors")
        print("monitors: ", monitors.__dict__)

        self._parse_monitors(monitors)
    except Exception as error:
        logger.error(f"Failed to get all monitors, error {error}")

def _fetch_tables(session, client) -> None:
    """Fetch all tables

    See https://apidocs.getmontecarlo.com/#query-getTables
    """
    print(session)

    batch_size = 500
    end_cursor = None
    result: List[Dict] = []

    while True:
        logger.info(f"Querying getTables after {end_cursor} ({len(result)} tables)")



        resp = client(
            """
            query getTables($first: Int, $after: String) {
                getTables(first: $first after: $after) {
                edges {
                    node {
                        mcon
                        warehouse {
                            connectionType
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
                }
            }
            """,
            {"first": batch_size, "after": end_cursor},
        )

        nodes = [edge["node"] for edge in resp["get_tables"]["edges"]]
        result.extend(nodes)

        if not resp["get_tables"]["page_info"]["has_next_page"]:
            break

        end_cursor = resp["get_tables"]["page_info"]["end_cursor"]

    logger.info(f"Fetched {len(result)} tables")
    #json_dump_to_debug_file(result, "getTables.json")

    for node in result:
        mcon = node["mcon"]
        connection_type = node["warehouse"]["connection_type"]
        platform = connection_type_platform_map.get(connection_type)
        if platform is None:
            logger.warning(
                f"Unsupported connection type: {connection_type} for {mcon}"
            )
        else:
            session._mcon_platform_map[mcon] = platform

    print("Session MCON Platform Map: ", session._mcon_platform_map)
    print("result: ", result.__dict__)
    

def create_assertions():

    dataset_urn = make_dataset_urn("snowflake", "Finance.public.securities", "PROD")

    assertion_urn = builder.make_assertion_urn(assertion_id="securities_ingest")

    assertion_key = AssertionKeyClass(
        assertionId='1234'
    )

    volume_assertion = VolumeAssertionInfoClass(
            type="ROW_COUNT_TOTAL",
            entity=dataset_urn,
            rowCountTotal=RowCountTotalClass(
                operator="GREATER_THAN",
                parameters=AssertionStdParametersClass(
                    value=AssertionStdParameterClass(
                        type="NUMBER",
                        value="10000"
                    )
                )
            )
        )

    assertion_info = AssertionInfoClass(
        type="VOLUME",
        volumeAssertion=volume_assertion
    )

    assertion_run_event = AssertionRunEventClass(
        timestampMillis=current_timestamp.time,
        runId="1234",
        asserteeUrn=dataset_urn,
        status="COMPLETE",
        assertionUrn=assertion_urn,
        result=AssertionResultClass(
            type="SUCCESS",
            rowCount=20000
        ),
    )

    assertion_mcps = MetadataChangeProposalWrapper.construct_many(
        entityUrn=assertion_urn,
        aspects=[
            assertion_info,
            assertion_key,
            assertion_run_event
        ]
    )

    for assertion_mcp in assertion_mcps:
        graph.emit(assertion_mcp)

def create_incidents():
    
        dataset_urn = make_dataset_urn("snowflake", "Finance.public.securities", "PROD")

        incident_key = IncidentKeyClass(
            id='1234'
        )
        incident_urn = f"urn:li:assertion:{incident_key.id}"

        incident_info = IncidentInfoClass(
            type="DATA_QUALITY",
            entities=[dataset_urn],
            status=IncidentStatusClass(               
                state="OPEN",
                lastUpdated=current_timestamp,
            ),
            created=current_timestamp,
            title="Data Quality Incident",
            description="Data Quality Incident Description",
            priority=1
        )

if __name__ == "__main__":

    session = ExtractorSession(
        id="1234",
        _mcon_platform_map={}
        )

    client=Client(
        session=Session(mcd_id="API Key ID", mcd_token="API Key Secret"),
    )

    _fetch_tables(session, client)
    _fetch_monitors(session, client)

    create_assertions()
    create_incidents()
