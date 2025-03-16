"""
Debezium connector configuration and management.
"""
import os
import json
import logging
import requests
from typing import Dict, Any

logger = logging.getLogger(__name__)


class DebeziumConnector:
    def __init__(self, connect_url: str = "http://localhost:8083"):
        self.connect_url = connect_url
        self.headers = {"Content-Type": "application/json"}

    def create_connector(self, table_name: str) -> Dict[str, Any]:
        """
        Create a Debezium connector for the specified table.
        If the connector already exists, it will check its status 
        and recreate it if it's in a failed state.
        """
        connector_name = f"finance-connector-{table_name}"

        # check if connector already exists before creating it
        # this avoids duplicate connectors and helps recover from failures
        existing_connectors = self.list_connectors()
        if connector_name in existing_connectors:
            status = self.get_connector_status(connector_name)
            
            if status.get("status") == "FAILED" or "error_code" in status:
                # if connector exists but is in a failed state, we delete and recreate it
                logger.warning(f"Connector {connector_name} is in a FAILED state. Recreating...")
                self.delete_connector(connector_name)
            else:
                # if connector exists and is working, we don't need to do anything
                logger.info(f"Connector {connector_name} already exists and is running. Skipping creation.")
                return status  # no need to recreate if it's working

        # connector doesn't exist or was deleted, so we create a new one
        return self._register_connector(table_name)

    def get_connector_status(self, connector_name: str) -> Dict[str, Any]:
        """
        Get the status of a specific connector.
        """
        try:
            response = requests.get(
                f"{self.connect_url}/connectors/{connector_name}/status",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get connector status: {str(e)}")
            raise

    def delete_connector(self, connector_name: str) -> None:
        """
        Delete a specific connector.
        """
        try:
            response = requests.delete(
                f"{self.connect_url}/connectors/{connector_name}",
                headers=self.headers
            )
            response.raise_for_status()
            logger.info(f"Successfully deleted connector {connector_name}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete connector: {str(e)}")
            raise

    def list_connectors(self) -> list:
        """
        ist all existing connectors.
        """
        try:
            response = requests.get(f"{self.connect_url}/connectors", headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list connectors: {str(e)}")
            return []

    def _register_connector(self, table_name: str) -> Dict[str, Any]:
        """
        Registers a connector.
        """
        connector_name = f"finance-connector-{table_name}"
        table_include = f"operations.{table_name}"

        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.hostname": "transactions-db",
                "database.port": "5432",
                "database.user": os.getenv("CDC_USER", "cdc_user"),
                "database.password": os.getenv("CDC_PASSWORD", "cdc_1234"),
                "database.dbname": "finance_db",
                "database.server.name": "finance",
                "topic.prefix": "finance",
                "table.include.list": table_include,
                "plugin.name": "pgoutput",
                "slot.name": "cdc_pgoutput",
                "publication.name": "cdc_publication"
            }
        }

        try:
            response = requests.post(
                f"{self.connect_url}/connectors",
                headers=self.headers,
                data=json.dumps(connector_config)
            )
            response.raise_for_status()
            logger.info(f"Successfully created connector for table {table_name}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create connector: {str(e)}")
            raise
