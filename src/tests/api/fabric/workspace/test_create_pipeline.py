from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from tests.utils import rand_workspace_item_name
from fabricengineer.api.fabric.workspace.items import (
    DataPipeline,
    Notebook,
    VariableLibrary,
    Warehouse,
    Lakehouse
)
from fabricengineer.api.fabric.workspace.base import BaseWorkspaceItem
from fabricengineer.api.fabric.workspace.create_pipeline import (
    WorkspaceItemCreationPipeline,
    PipelineItemStatus
)


class TestWorkspaceItemCreationPipeline:

    def items_to_create(self, folder: WorkspaceFolder, raise_error: bool = False) -> list[BaseWorkspaceItem]:
        workspace_id = folder.item.api.workspaceId
        sub_folder = WorkspaceFolder(
            workspace_id,
            name="" if raise_error else rand_workspace_item_name("F"),
            parent_folder=folder
        )
        data_pipeline = DataPipeline(
            workspace_id,
            name=rand_workspace_item_name("DP"),
            folder=sub_folder
        )
        notebook = Notebook(
            workspace_id,
            name=rand_workspace_item_name("NB"),
            folder=sub_folder
        )
        variable_library = VariableLibrary(
            workspace_id,
            name=rand_workspace_item_name("VL"),
            folder=sub_folder
        )
        warehouse = Warehouse(
            workspace_id,
            name=rand_workspace_item_name("WH"),
            folder=sub_folder
        )
        lakehouse = Lakehouse(
            workspace_id,
            name=rand_workspace_item_name("LH"),
            folder=sub_folder
        )

        return [
            sub_folder,
            data_pipeline,
            notebook,
            variable_library,
            warehouse,
            lakehouse
        ]

    def test_run_in_sequence_and_multiple_run(self, folder_singleton: WorkspaceFolder):
        items = self.items_to_create(folder_singleton)
        assert len(items) == 6

        pipeline = WorkspaceItemCreationPipeline(items)
        result_run_1 = pipeline.run(in_parallel=False)

        assertion_status = (PipelineItemStatus.CREATED, PipelineItemStatus.SKIPPED_EXISTS)
        assert result_run_1 is not None
        assert len(result_run_1.errors) == 0
        assert all(
            item.status in assertion_status
            for item in result_run_1.pipeline_items
        )

        # 2. run
        result_run_2 = pipeline.run(in_parallel=True)
        assert result_run_2 is not None
        assert len(result_run_2.errors) == 0
        assert all(
            item.status in assertion_status
            for item in result_run_2.pipeline_items
        )

    def test_run_in_parallel_and_multiple_run(self, folder_singleton: WorkspaceFolder):
        items = self.items_to_create(folder_singleton)
        assert len(items) == 6

        pipeline = WorkspaceItemCreationPipeline(items)
        result_run_1 = pipeline.run(in_parallel=True)

        assertion_status = (PipelineItemStatus.CREATED, PipelineItemStatus.SKIPPED_EXISTS)
        assert result_run_1 is not None
        assert len(result_run_1.errors) == 0
        assert all(
            item.status in assertion_status
            for item in result_run_1.pipeline_items
        )

        # 2. run
        result_run_2 = pipeline.run(in_parallel=True)
        assert result_run_2 is not None
        assert len(result_run_2.errors) == 0
        assert all(
            item.status in assertion_status
            for item in result_run_2.pipeline_items
        )

    def test_run_in_parallel_fail(self, folder_singleton: WorkspaceFolder):
        items = self.items_to_create(folder_singleton, raise_error=True)
        assert len(items) == 6

        pipeline = WorkspaceItemCreationPipeline(items)
        result = pipeline.run(in_parallel=False)

        assert result is not None
        assert len(result.errors) == 6
        assert len([i for i in result.pipeline_items if i.status == PipelineItemStatus.SKIPPED_EXISTS]) == 1
        assert len([i for i in result.pipeline_items if i.status == PipelineItemStatus.FAILED]) == 6

    def test_run_in_sequence_fail_deadlock(self, folder_singleton: WorkspaceFolder):
        items = self.items_to_create(folder_singleton, raise_error=True)
        assert len(items) == 6

        pipeline = WorkspaceItemCreationPipeline(items)
        result = pipeline.run(in_parallel=False)

        assert result is not None
        assert len(result.errors) == 6
        assert len([i for i in result.pipeline_items if i.status == PipelineItemStatus.SKIPPED_EXISTS]) == 1
        assert len([i for i in result.pipeline_items if i.status == PipelineItemStatus.FAILED]) == 6

    def test_run_in_sequence_fail(self, folder_singleton: WorkspaceFolder):
        items = self.items_to_create(folder_singleton, raise_error=False)
        assert len(items) == 6
        for item in items[1:]:
            item.item.fields["displayName"] = None

        pipeline = WorkspaceItemCreationPipeline(items)
        result = pipeline.run(in_parallel=False)

        assert result is not None
        assert len(result.errors) == 5
        assert len([i for i in result.pipeline_items if i.status == PipelineItemStatus.FAILED]) == 5
