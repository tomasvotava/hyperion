import datetime
import json
from typing import Any, TextIO

import click

from hyperion.config import queue_config
from hyperion.log import get_logger
from hyperion.sources.base import Source, SourceParamsType

logger = get_logger("hyperion-source-runner")


def _enforce_file_queue(queue_path: str, *, path_overwrite: bool) -> None:
    if queue_config.url is not None:
        logger.warning(
            "Direct (aka Argo Workflows) run mode requires FileQueue, "
            "but SQS queue was configured. Configuration will be discarded. "
            "If this is breaking your use-case, please open an issue.",
            url="https://github.com/tomasvotava/hyperion/issues/new",
        )
    queue_config.url = None
    queue_config.path = queue_path
    queue_config.path_overwrite = path_overwrite


def _resolve_params(params: str | None, params_from: TextIO | None) -> SourceParamsType | None:
    if params is not None and params_from is not None:
        raise ValueError("Only one of --params or --params-from may be specified, not both.")
    loaded: Any = None
    if params is not None:
        loaded = json.loads(params)
    elif params_from is not None:
        loaded = json.load(params_from)
    if not isinstance(loaded, list | dict) and loaded is not None:
        raise ValueError("Invalid parameters provided, not a valid json object nor array.")
    return loaded


class SourceRunner:
    def __init__(self, *sources: type[Source]) -> None:
        self.sources = {source.source: source for source in sources}

    def cli(self) -> None:
        @click.group()
        def cli() -> None:
            pass

        for name, source_cls in self.sources.items():
            logger.debug("Registering CLI handlers for source.", source=name, source_cls=source_cls)
            group = click.Group(name=name)

            @group.command("run")
            @click.option("--start-date", type=click.DateTime(), default=None)
            @click.option("--end-date", type=click.DateTime(), default=None)
            @click.option("--params", type=click.STRING, default=None)
            @click.option("--params-from", type=click.File("r"), default=None)
            @click.option(
                "--queue-file",
                type=click.Path(file_okay=True, dir_okay=False, writable=True, resolve_path=True),
                default=None,
            )
            @click.option("--queue-overwrite", type=click.BOOL, default=False, is_flag=True)
            def argo_run(
                start_date: datetime.datetime | None = None,
                end_date: datetime.datetime | None = None,
                params: str | None = None,
                params_from: TextIO | None = None,
                queue_file: str | None = None,
                queue_overwrite: bool = False,
                source_cls: type[Source] = source_cls,
            ) -> None:
                source_params = _resolve_params(params, params_from)

                if queue_file:
                    _enforce_file_queue(queue_file, path_overwrite=queue_overwrite)

                source_cls.handle_argo_workflow_run(start_date=start_date, end_date=end_date, params=source_params)

            cli.add_command(group)
        cli()
