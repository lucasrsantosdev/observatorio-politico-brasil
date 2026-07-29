from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.dimensoes_gastos_deputados import (
    build_dim_beneficiario,
    build_dim_fornecedor,
    build_dim_movimento,
    build_dim_partido,
    build_dim_tempo,
    build_dim_tipo_despesa,
    build_dim_uf,
)

logger = logging.getLogger(__name__)


def _write_dimension(
    *,
    dataframe: pl.DataFrame,
    destination_root: Path,
    dimension: str,
) -> dict[str, object]:
    destination = destination_root / dimension

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dimension}.parquet"

    csv_path = destination / f"{dimension}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    logger.info(
        "Dimensão de gastos criada: dimensão=%s registros=%s",
        dimension,
        dataframe.height,
    )

    return {
        "dimension": dimension,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_dimensions_gastos_deputados(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    years_label = "_".join(str(year) for year in normalized_years)

    gold_root = (
        Path("data/gold")
        / "camara_deputados"
        / "gastos_deputados"
        / f"anos={years_label}"
    )

    reconciliation_path = gold_root / "reconciliation" / "reconciliation.manifest.json"

    if not reconciliation_path.exists():
        raise FileNotFoundError("Manifesto de reconciliação não encontrado.")

    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))

    if not reconciliation.get(
        "approved",
        False,
    ):
        raise ValueError("A reconciliação de gastos não está aprovada.")

    fato = pl.read_parquet(
        gold_root / "fato_gastos_deputados" / "fato_gastos_deputados.parquet"
    )

    dimensions = {
        "dim_beneficiario": (build_dim_beneficiario(fato)),
        "dim_fornecedor": (build_dim_fornecedor(fato)),
        "dim_tipo_despesa": (build_dim_tipo_despesa(fato)),
        "dim_partido": (build_dim_partido(fato)),
        "dim_uf": (build_dim_uf(fato)),
        "dim_movimento": (build_dim_movimento(fato)),
        "dim_tempo": (build_dim_tempo(fato)),
    }

    destination_root = gold_root / "dimensions"

    outputs = [
        _write_dimension(
            dataframe=dataframe,
            destination_root=destination_root,
            dimension=dimension,
        )
        for dimension, dataframe in dimensions.items()
    ]

    manifest = {
        "source": "camara_deputados",
        "subject": "gastos_deputados_ceap",
        "layer": "gold_dimensions",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_approved": True,
        "dimensions": outputs,
    }

    manifest_path = destination_root / "dimensions.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Dimensões de gastos concluídas: %s",
        manifest_path,
    )

    return manifest_path
