from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone

class MongoDBClient:
    def __init__(self, settings):
        self.settings = settings
        self.client: AsyncIOMotorClient | None = None
        self.database = None
        self.collection = None


    def _connection_uri(self) -> str:
        mongo = self.settings.mongo

        if mongo.user and mongo.password:
            return (
                f"mongodb://{mongo.user}:{mongo.password}"
                f"@{mongo.host}:{mongo.port}/"
                f"?authSource={mongo.auth_db or 'admin'}"
            )

        return f"mongodb://{mongo.host}:{mongo.port}/"


    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self._connection_uri())

        await self.client.admin.command("ping")

        db_name = self.settings.project.mongo_database
        collection_name = self.settings.project.mongo_collection

        self.database = self.client[db_name]
        self.collection = self.database[collection_name]

        print("[MongoDB] is connected")


    async def disconnect(self) -> None:
        if self.client:
            self.client.close()
            print("[MongoDB] is disconnected")


    async def write_many(self, documents: list[dict]) -> None:
        if self.collection is None:
            raise RuntimeError("[MongoDB] is not connected")

        if not documents:
            return

        mongo_documents = [
            self.transform(document)
            for document in documents
        ]

        await self.collection.insert_many(mongo_documents)

    
    def transform(self, document: dict) -> dict:
        """
        Transform internal pipeline documents into a MongoDB optimized structure.

        MongoDB is used as the primary structured data storage for VIDS.
        Therefore the document structure should remain close to the original
        Asset Administration Shell (AAS) representation.

        Design goals:
        - preserve semantic structure
        - preserve submodels and submodel elements
        - enable later reconstruction of AAS-like documents
        - support flexible schema evolution
        - support AI/data-science workloads

        Target MongoDB document:
        {
            "project": "Demo",
            "organisation": "TH-Nuernberg",
            "asset": "Machine_1",
            "asset_id": "...",
            "asset_id_type": "uuid",
            "topic": "TH-Nuernberg/Demo/Machine_1",
            "timestamp": datetime,
            "received_at": datetime,
            "submodels": {
                "TechnicalData": {
                    "ProcTime": {
                        "value": 45,
                        "valueType": "xs:duration",
                        "unit": "s",
                    },
                },
                "OperationalData": {
                    "StatWorkingPortion": {
                        "value": 0.605,
                        "valueType": "xs:double",
                    },
                },
            },
            "raw": {...}
        }

        Important:
        - timestamp = simulation timestamp
        - received_at = ingestion timestamp
        - raw contains the original MQTT payload
        """

        payload = document.get("payload", {})
        shell = payload.get("assetAdministrationShell", {})

        return {
            "project": shell.get("idProject"),
            "organisation": shell.get("idOrganisation"),
            "asset": shell.get("idShort"),
            "asset_id": shell.get("identification", {}).get("id"),
            "asset_id_type": shell.get("identification", {}).get("idType"),
            "topic": document.get("topic"),
            "timestamp": self._parse_datetime(self._extract_timestamp(shell)),
            "received_at": document.get("received_at"),
            "submodels": self._extract_submodels(shell),
            "raw": payload,
        }


    def _extract_timestamp(self, shell: dict) -> str | None:
        timestamp = shell.get("timestamp")

        if isinstance(timestamp, dict):
            return timestamp.get("value")

        return None


    def _extract_submodels(self, shell: dict) -> dict:
        submodels = {}

        for submodel in shell.get("Submodels", []):
            submodel_name = submodel.get("idShort")

            if not submodel_name:
                continue

            elements = {}

            for element in submodel.get("submodelElements", []):
                element_name = element.get("idShort")

                if not element_name:
                    continue

                value = {
                    "value": element.get("value"),
                    "valueType": element.get("valueType"),
                }

                unit = element.get("unit")
                if unit and unit.lower() != "none":
                    value["unit"] = unit

                elements[element_name] = value

            submodels[submodel_name] = elements

        return submodels
    
    def _parse_datetime(self, value: str | None):
        if not value:
            return None

        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")

            return datetime.fromisoformat(value).astimezone(timezone.utc)

        except ValueError:
            return value
