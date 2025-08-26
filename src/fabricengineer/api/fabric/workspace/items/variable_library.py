from typing import Optional, Any
from dataclasses import dataclass, field, asdict

from fabricengineer.api.utils import base64_encode
from fabricengineer.api.fabric.workspace.base import (
    BaseWorkspaceItem,
    BaseItemAPIData,
    FabricItem,
    ItemDefinitionInterface,
    WorkspaceItemDependency,
    WorkspaceItemDependencyType
)
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder


ITEM_PATH = "/VariableLibraries"


@dataclass
class VariableLibraryProperties:
    activeValueSetName: Optional[str] = None


@dataclass
class VariableLibraryAPIData(BaseItemAPIData):
    folderId: str = None
    properties: VariableLibraryProperties = field(default_factory=VariableLibraryProperties)


@dataclass
class VariableLibraryVariable:
    name: str
    note: str
    type: str
    value: Any | WorkspaceItemDependency


class VariableLibraryDefinition(ItemDefinitionInterface):
    def __init__(
            self,
            value_sets_ordered: list[str],
            *variables: list[VariableLibraryVariable]
    ) -> None:
        self._value_sets = value_sets_ordered
        self._variables: list[VariableLibraryVariable] = variables or []

    def get_definition(self) -> dict:
        variables = self._serialize_variables()
        variables_data = self._build_variables_data(variables)
        settings_data = self._build_settings_data()
        platform_data = self._build_platform_data()

        variable_sets_data_b64 = self._build_variable_sets_data_b64(variables)
        variables_data_b64 = base64_encode(variables_data)
        settings_data_b64 = base64_encode(settings_data)
        platform_data_b64 = base64_encode(platform_data)

        parts = [
            {
                "path": "variables.json",
                "payload": variables_data_b64,
                "payloadType": "InlineBase64"
            },
            {
                "path": "settings.json",
                "payload": settings_data_b64,
                "payloadType": "InlineBase64"
            }
        ] + variable_sets_data_b64 + [
            {
                "path": ".platform",
                "payload": platform_data_b64,
                "payloadType": "InlineBase64"
            }
        ]

        return {
            "parts": parts
        }

    def _serialize_variables(self) -> list[dict]:
        return [asdict(var) for var in self._variables]

    def _build_variables_data(self, variables: list[dict]) -> dict:
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json",
            "variables": variables
        }

    def _build_settings_data(self) -> dict:
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/settings/1.0.0/schema.json",
            "valueSetsOrder": self._value_sets
        }

    def _build_platform_data(self) -> dict:
        return {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "VariableLibrary",
                "displayName": "VariableLibrary"
            },
            "config": {
                "version": "2.0",
                "logicalId": "00000000-0000-0000-0000-000000000000"
            }
        }

    def _build_variable_sets_data_b64(self, variables: list[dict]) -> list[dict]:
        return [
            {
                "path": f"valueSets/{value_set}.json",
                "payload": base64_encode({
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json",
                    "name": value_set,
                    "variableOverrides": [
                        {"name": var["name"], "value": var["value"]}
                        for var in variables
                    ]
                }),
                "payloadType": "InlineBase64"
            }
            for value_set in self._value_sets
        ]


class VariableLibrary(BaseWorkspaceItem[VariableLibraryAPIData]):
    """
    REF: https://learn.microsoft.com/en-us/rest/api/fabric/variablelibrary/items
    """
    def __init__(
        self,
        workspace_id: str,
        name: str,
        description: str = None,
        folder: WorkspaceFolder = None,
        definition: VariableLibraryDefinition = None,
        api_data: VariableLibraryAPIData = None
    ):
        depends_on = None
        folder = None if not isinstance(folder, WorkspaceFolder) else folder
        if folder is not None:
            depends_on = [
                WorkspaceItemDependency(folder, WorkspaceItemDependencyType.FOLDER, "folderId")
            ]
        self._init_definition = definition
        definition = definition.get_definition() if isinstance(definition, ItemDefinitionInterface) else None
        description = description or "New VariableLibrary"
        item = FabricItem[VariableLibraryAPIData](
            displayName=name,
            description=description,
            folderId=folder,
            definition=definition,
            api_data=api_data
        )
        super().__init__(
            create_type_fn=VariableLibrary.from_json,
            base_item_url=ITEM_PATH,
            workspace_id=workspace_id,
            item=item,
            depends_on=depends_on
        )

    @staticmethod
    def from_json(item: dict) -> "VariableLibrary":
        kwargs = item.copy()
        if "properties" not in item.keys():
            item["properties"] = {}
        kwargs["properties"] = VariableLibraryProperties(**item["properties"])
        api_data = VariableLibraryAPIData(**kwargs)
        return VariableLibrary(
            workspace_id=api_data.workspaceId,
            name=api_data.displayName,
            description=api_data.description,
            api_data=api_data
        )

    @staticmethod
    def get_by_name(workspace_id: str, name: str) -> "VariableLibrary":
        return BaseWorkspaceItem.get_by_name(
            create_item_type_fn=VariableLibrary.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            name=name
        )

    @staticmethod
    def get_by_id(workspace_id: str, id: str) -> "VariableLibrary":
        return BaseWorkspaceItem.get_by_id(
            create_item_type_fn=VariableLibrary.from_json,
            workspace_id=workspace_id,
            base_item_url=ITEM_PATH,
            id=id
        )

    @staticmethod
    def list(workspace_id: str) -> list["VariableLibrary"]:
        return [
            VariableLibrary.from_json(item)
            for item in BaseWorkspaceItem.list(
                workspace_id=workspace_id,
                base_item_url=ITEM_PATH
            )
        ]
