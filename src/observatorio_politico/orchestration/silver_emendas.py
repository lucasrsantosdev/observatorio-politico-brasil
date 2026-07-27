from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from observatorio_politico.config import Settings
from observatorio_politico.transformations.emendas import (
    transform_emendas,
)

logger = logging.getLogger(__name__)


def find_latest_bronze_execution(
    *,
    bronze_root: Path,
    ano: int,
) -> Path:
    base_path = bronze_root / "portal_transparencia" / "emendas" / f"ano_emenda={ano}"

    execution_paths = sorted(
        (path.parent for path in base_path.rglob("execucao.manifest.json")),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not execution_paths:
        raise FileNotFoundError(f"Nenhuma execução Bronze encontrada para {ano}.")

    return execution_paths[0]


def run_silver_emendas(
    settings: Settings,
    *,
    ano: int,
    execution_path: Path | None = None,
) -> tuple[Path, Path, Path]:
    execution_time = datetime.now(UTC)

    bronze_execution = (
        execution_path
        if execution_path is not None
        else find_latest_bronze_execution(
            bronze_root=settings.bronze_path,
            ano=ano,
        )
    )

    logger.info(
        "Transformando Bronze em Silver: %s",
        bronze_execution,
    )

    dataframe = transform_emendas(
        execution_path=bronze_execution,
    )

    destination = (
        Path("data/silver") / "portal_transparencia" / "emendas" / f"ano={ano}"
    )
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "emendas.parquet"
    csv_path = destination / "emendas.csv"
    manifest_path = destination / "silver.manifest.json"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
        include_header=True,
    )

    manifest = {
        "source": "portal_transparencia",
        "entity": "emendas",
        "layer": "silver",
        "ano": ano,
        "bronze_execution": str(bronze_execution),
        "processed_at_utc": execution_time.isoformat(),
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Silver concluída: registros=%s parquet=%s",
        dataframe.height,
        parquet_path,
    )

    return parquet_path, csv_path, manifest_path
