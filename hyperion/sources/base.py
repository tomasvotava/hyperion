"""Base abstract class for sources."""

import abc
import asyncio
import datetime
from collections.abc import AsyncIterator, Awaitable, Iterable
from dataclasses import dataclass
from typing import Any, ClassVar, TypeAlias, cast

from aws_lambda_typing.context import Context
from aws_lambda_typing.events import EventBridgeEvent, SQSEvent

from hyperion.asyncutils import AsyncTaskQueue, get_loop
from hyperion.catalog import Catalog
from hyperion.config import source_config, storage_config
from hyperion.entities.catalog import DataLakeAsset
from hyperion.infrastructure.message_queue import (
    FileQueue,
    Queue,
    SourceBackfillMessage,
    SQSQueue,
    iter_messages_from_sqs_event,
)
from hyperion.log import get_logger

SourceEventType = EventBridgeEvent | SQSEvent

SourceParamsType: TypeAlias = dict[str, Any] | list[Any]

logger = get_logger("hyperion-source")


def _warn_duplicate_params_config() -> None:
    logger.warning(
        "Source params were set both in the configuration and on the caller, environment configuration will be ignored."
    )


@dataclass(eq=True, frozen=True)
class SourceAsset:
    asset: DataLakeAsset
    data: Iterable[dict[str, Any]]
    schema_path: str | None = None


class Source(abc.ABC):
    source: ClassVar[str] = NotImplemented

    def __init__(self, catalog: Catalog) -> None:
        if self.source is NotImplemented:
            raise NotImplementedError("Cannot instantiate a source without a source name.")
        self.catalog = catalog

    @abc.abstractmethod
    def run(
        self,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        params: SourceParamsType | None = None,
    ) -> Awaitable[Iterable[SourceAsset]] | AsyncIterator[SourceAsset]:
        """The main coroutine that runs the source extraction."""

    @classmethod
    async def _run(
        cls,
        catalog: Catalog,
        notify: bool = True,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        params: SourceParamsType | None = None,
    ) -> None:
        source = cls(catalog)
        result = source.run(start_date=start_date, end_date=end_date, params=params)
        async with AsyncTaskQueue[None](maxsize=storage_config.max_concurrency) as queue:
            if isinstance(result, AsyncIterator):
                async for asset in result:
                    logger.info("Processing asset retrieved from source.", asset=asset.asset)
                    await queue.add_task(
                        source.catalog.store_asset_async(
                            asset.asset, asset.data, notify=notify, schema_path=asset.schema_path
                        )
                    )
            else:
                for asset in await result:
                    logger.info("Processing asset retrieved from source.", asset=asset.asset)
                    await queue.add_task(
                        source.catalog.store_asset_async(
                            asset.asset, asset.data, notify=notify, schema_path=asset.schema_path
                        )
                    )

    @classmethod
    def handle_aws_lambda_event(
        cls,
        event: SourceEventType | None = None,
        context: Context | None = None,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        params: SourceParamsType | None = None,
    ) -> None:
        logger.info(
            "Starting Hyperion source in AWS Lambda mode.", source=cls.__name__, event=str(event), context=str(context)
        )
        if params and source_config.params:
            _warn_duplicate_params_config()
        params = params or source_config.params
        catalog = Catalog.from_config()
        loop = loop or get_loop()
        queue = SQSQueue.from_config()
        if isinstance(event, dict) and "Records" in event:
            # We may presume this is an SQS Event
            event = cast(SQSEvent, event)
            for message in iter_messages_from_sqs_event(event):
                if not isinstance(message, SourceBackfillMessage):
                    logger.warning(
                        "Only SourceBackfillMessage is supported, this message is probably not for us.", message=message
                    )
                    continue
                if message.source != cls.source:
                    logger.info("Message is not intended for us.", source=cls.source, message_source=message.source)
                    continue
                logger.info("Source triggered by an SQS Message.", source=cls.source, message=message)
                loop.run_until_complete(
                    cls._run(
                        catalog,
                        start_date=message.start_date,
                        end_date=message.end_date,
                        notify=message.notify,
                        params=params,
                    )
                )
                if message.receipt_handle:
                    queue.delete(message.receipt_handle)
            return
        if isinstance(event, dict) and "detail" in event:
            # We may presume this is an EventBridgeEvent
            event = cast(EventBridgeEvent, event)
            logger.warning("EventBridge events can carry no config for now.")
            loop.run_until_complete(cls._run(catalog, start_date=None, end_date=None, params=params))
            return
        logger.warning("No event was provided, assuming a no-config run.")
        loop.run_until_complete(cls._run(catalog, start_date=None, end_date=None, params=params))

    @classmethod
    def handle_argo_workflow_run(
        cls,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        params: SourceParamsType | None = None,
    ) -> None:
        logger.info("Starting Hyperion source in Argo Workflow mode.", source=cls.__name__)
        queue = Queue.from_config()
        if not isinstance(queue, FileQueue):
            raise RuntimeError(
                "In Argo Workflow mode only FileQueue is allowed. Make sure you've set HYPERION_QUEUE_PATH "
                "env variable to a writable file in the Argo artifact directory."
            )
        if params and source_config.params:
            _warn_duplicate_params_config()
        params = params or source_config.params
        catalog = Catalog.from_config()
        loop = loop or get_loop()
        with queue:
            loop.run_until_complete(
                cls._run(catalog, start_date=start_date, end_date=end_date, params=params, notify=True)
            )
