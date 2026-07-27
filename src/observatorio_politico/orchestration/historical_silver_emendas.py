from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.config import Settings
from observatorio_politico.transformations.historical_emendas import (
    convert_cp1252_to_utf8,
    transform_convenios,
    transform_emendas,
    transform_favorecidos,
)

logger = logging.getLogger(__name__)


ENTITIES = {
    "emendas": {
        "bronze_directory": "emendas_historico",
        "filename": "EmendasParlamentares.csv",
        "transformer": transform_emendas,
    },
    "convenios": {
        "bronze_directory": "emendas_convenios_historico",
        "filename": "EmendasParlamentares_Convenios.csv",
        "transformer": transform_convenios,
    },
    "favorecidos": {
        "bronze_directory": "emendas_favorecidos_historico",
        "filename": "EmendasParlamentares_PorFavorecido.csv",
        "transformer": transform_favorecidos,
    },
}


def _find_latest_csv(
    *,
    bronze_root: Path,
    bronze_directory: str,
    filename: str,
) -> Path:
    base_path = (
        bronze_root / "portal_transparencia" / bronze_directory / "todos_os_anos"
    )

    candidates = sorted(
        base_path.rglob(filename),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(f"Arquivo histórico não encontrado: {filename}")

    return candidates[0]


def _write_outputs(
    *,
    dataframe: pl.DataFrame,
    destination: Path,
    entity: str,
) -> dict[str, object]:
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{entity}.parquet"
    csv_path = destination / f"{entity}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
    )
    dataframe.write_csv(
        csv_path,
        separator=";",
        include_header=True,
    )

    return {
        "entity": entity,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_historical_silver_emendas(
    settings: Settings,
    *,
    years: list[int],
) -> Path:
    execution_time = datetime.now(UTC)

    destination = (
        Path("data/silver")
        / "portal_transparencia"
        / "historico_emendas"
        / f"anos={'_'.join(str(year) for year in years)}"
    )
    staging_directory = Path("data/silver/_staging") / "historico_emendas"

    outputs: list[dict[str, object]] = []

    for entity, configuration in ENTITIES.items():
        source_file = _find_latest_csv(
            bronze_root=settings.bronze_path,
            bronze_directory=str(configuration["bronze_directory"]),
            filename=str(configuration["filename"]),
        )

        utf8_file = staging_directory / f"{entity}_utf8.csv"

        logger.info(
            "Preparando entidade=%s origem=%s",
            entity,
            source_file,
        )

        convert_cp1252_to_utf8(
            source_file,
            utf8_file,
        )

        transformer: Callable[..., pl.DataFrame] = configuration["transformer"]

        dataframe = transformer(
            utf8_file,
            years=years,
        )

        output = _write_outputs(
            dataframe=dataframe,
            destination=destination / entity,
            entity=entity,
        )
        outputs.append(output)

        logger.info(
            "Silver histórica concluída: entidade=%s registros=%s",
            entity,
            dataframe.height,
        )

    manifest = {
        "source": "portal_transparencia",
        "layer": "silver",
        "load_type": "historical",
        "years": years,
        "processed_at_utc": execution_time.isoformat(),
        "entities": outputs,
    }

    manifest_path = destination / "silver.manifest.json"
    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Silver histórica completa: manifesto=%s",
        manifest_path,
    )

    return manifest_path
