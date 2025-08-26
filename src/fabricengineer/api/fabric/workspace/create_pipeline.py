import os
import time

from enum import Enum
from concurrent.futures import ThreadPoolExecutor

from fabricengineer.logging import logger
from fabricengineer.api.fabric.workspace.base import BaseWorkspaceItem


class PipelineItemStatus(Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SKIPPED_EXISTS = "SKIPPED_EXISTS"
    CREATED = "CREATED"
    FAILED = "FAILED"


class PipelineItem:
    """Represents an item in the creation pipeline with its status and dependencies."""
    def __init__(
            self,
            item: BaseWorkspaceItem,
            finished_pipeline_items: set[BaseWorkspaceItem]
    ):
        self._item = item
        self._finished_pipeline_items = finished_pipeline_items
        self._status = PipelineItemStatus.QUEUED
        self._error = None

    @property
    def item(self) -> BaseWorkspaceItem:
        return self._item

    @property
    def status(self) -> PipelineItemStatus:
        return self._status

    def is_ready(self) -> bool:
        """Check if all upstream dependencies are created."""
        return all(
            dep.item in self._finished_pipeline_items
            for dep in self._item._upstream_items
        )

    def create_if_not_exists(self) -> None:
        """Create the item if it does not already exist, respecting dependencies."""
        while not self.is_ready():
            time.sleep(1)

        self._status = PipelineItemStatus.RUNNING
        logger.info(f"Start creating item {self._item}")
        try:
            if self._item.exists():
                self._status = PipelineItemStatus.SKIPPED_EXISTS
                return
            self._item.create()
            self._status = PipelineItemStatus.CREATED
        except Exception as e:
            self._status = PipelineItemStatus.FAILED
            self._error = str(e)
        finally:
            self._finished_pipeline_items.add(self._item)
            if self._status != PipelineItemStatus.FAILED:
                logger.info(f"Finished creating item {self._item} with status {self._status}")
            else:
                logger.error(f"Failed creating item {self._item} with error: {self._error}")

    def __str__(self):
        return f"PipelineItem(item={self._item}, status={self._status}, error={self._error})"

    def __repr__(self):
        return self.__str__()


class CreationPipelineResult:
    def __init__(self, items: list[PipelineItem]):
        self.pipeline_items = items
        self.errors = [item for item in items if item.status == PipelineItemStatus.FAILED]
        self.results = [
            {
                "item": item.item.item,
                "status": item.status
            }
            for item in items
        ]

    def __str__(self):
        return str(self.results)

    def __repr__(self):
        return f"CreationPipelineResult(results={self.results}, errors={self.errors})"


class WorkspaceItemCreationPipeline:
    def __init__(self, items: list[BaseWorkspaceItem] = None):
        self.set_items(items)

    @property
    def items(self) -> list[BaseWorkspaceItem]:
        return self._items

    def set_items(self, items: list[BaseWorkspaceItem]) -> "WorkspaceItemCreationPipeline":
        self._items = items
        self._prepare_items()
        return self

    def run(
            self,
            *,
            in_parallel: bool = True,
            max_workers: int = None
    ) -> CreationPipelineResult:
        """Run the creation pipeline for the workspace items."""
        finished_pipeline_items = set()
        pipeline_items = [
            PipelineItem(item, finished_pipeline_items)
            for item in self._items
        ]

        result = None
        if in_parallel:
            result = self._run_in_parallel(pipeline_items, max_workers=max_workers)
        else:
            result = self._run_in_sequence(pipeline_items)
        return result

    def _run_in_sequence(self, pipeline_items: list[PipelineItem]) -> CreationPipelineResult:
        """Run the creation pipeline in sequence."""
        run_items = pipeline_items.copy()
        while len(run_items) > 0:
            for item in run_items:
                if not item.is_ready():
                    continue
                item.create_if_not_exists()
                run_items.remove(item)
        return CreationPipelineResult(pipeline_items)

    def _run_in_parallel(
            self,
            pipeline_items: list[PipelineItem],
            max_workers: int = None
    ) -> CreationPipelineResult:
        """Run the creation pipeline in parallel using a thread pool."""
        cpu = os.cpu_count() or 4
        max_workers = max_workers or min(32, cpu * 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            _ = executor.map(self._execute_pipeline_item, pipeline_items)
        return CreationPipelineResult(pipeline_items)

    def _execute_pipeline_item(self, pipeline_item: PipelineItem):
        """Execute the creation of a single pipeline item."""
        pipeline_item.create_if_not_exists()

    def _prepare_items(self) -> None:
        """Prepare the items by resolving and adding any missing dependencies."""
        if self._items is None:
            self._items = []
            return
        depends_on_items = []
        for item in self._items:
            depends_on_items.extend([it.item for it in item.upstream_items])

        depends_on_items = set(depends_on_items)
        for item in depends_on_items:
            if item not in self._items:
                self._items.append(item)
