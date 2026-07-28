from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.dimensions_emendas import (
    build_dim_ano,
    build_dim_autor,
    build_dim_favorecido,
    build_dim_funcao,
    build_dim_municipio,
    build_dim_uf,
)

logger = logging.getLogger(__name__)


def _read_gold_dataset(
    gold_root: Path,
    name: str,
) -> pl.DataFrame:
    path = gold_root / name / f"{name}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset Gold não encontrado: {path}")

    return pl.read_parquet(path)


def _write_dimension(
    dataframe: pl.DataFrame,
    destination: Path,
    name: str,
) -> dict[str, object]:
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{name}.parquet"
    csv_path = destination / f"{name}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
        include_header=True,
    )

    logger.info(
        "Dimensão criada: nome=%s registros=%s",
        name,
        dataframe.height,
    )

    return {
        "dimension": name,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_dimensions_emendas(
    *,
    years: list[int],
) -> Path:
    years_label = "_".join(str(year) for year in years)

    gold_root = (
        Path("data/gold") / "portal_transparencia" / "emendas" / f"anos={years_label}"
    )

    ranking_parlamentares = _read_gold_dataset(
        gold_root,
        "ranking_parlamentares",
    )

    ranking_favorecidos = _read_gold_dataset(
        gold_root,
        "ranking_favorecidos",
    )

    ranking_funcoes = _read_gold_dataset(
        gold_root,
        "ranking_funcoes",
    )

    ranking_municipios = _read_gold_dataset(
        gold_root,
        "ranking_municipios",
    )

    ranking_uf = _read_gold_dataset(
        gold_root,
        "ranking_uf",
    )

    relacionamento = _read_gold_dataset(
        gold_root,
        "relacionamento_autor_favorecido",
    )

    dimensions = {
        "dim_ano": build_dim_ano(
            [
                ranking_parlamentares,
                ranking_favorecidos,
                ranking_funcoes,
                ranking_municipios,
                ranking_uf,
                relacionamento,
            ]
        ),
        "dim_autor": build_dim_autor(
            ranking_parlamentares,
            relacionamento,
        ),
        "dim_favorecido": build_dim_favorecido(
            ranking_favorecidos,
            relacionamento,
        ),
        "dim_funcao": build_dim_funcao(
            ranking_funcoes,
        ),
        "dim_uf": build_dim_uf(
            ranking_uf,
            ranking_municipios,
        ),
        "dim_municipio": build_dim_municipio(
            ranking_municipios,
        ),
    }

    destination = gold_root / "dimensions"

    outputs: list[dict[str, object]] = []

    for name, dataframe in dimensions.items():
        output = _write_dimension(
            dataframe=dataframe,
            destination=destination / name,
            name=name,
        )
        outputs.append(output)

    manifest = {
        "subject": "emendas",
        "layer": "gold",
        "dataset_type": "dimensions",
        "years": years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "dimensions": outputs,
        "relationships": [
            {
                "dimension": "dim_ano",
                "key": "ano_emenda",
                "targets": [
                    "ranking_parlamentares",
                    "ranking_favorecidos",
                    "ranking_funcoes",
                    "ranking_municipios",
                    "ranking_uf",
                    "relacionamento_autor_favorecido",
                ],
            },
            {
                "dimension": "dim_autor",
                "key": "codigo_autor_emenda",
                "targets": [
                    "ranking_parlamentares",
                    "relacionamento_autor_favorecido",
                ],
            },
            {
                "dimension": "dim_favorecido",
                "key": "codigo_favorecido",
                "targets": [
                    "ranking_favorecidos",
                    "relacionamento_autor_favorecido",
                ],
            },
            {
                "dimension": "dim_funcao",
                "key": "codigo_funcao",
                "targets": [
                    "ranking_funcoes",
                ],
            },
            {
                "dimension": "dim_uf",
                "key": "uf",
                "targets": [
                    "ranking_uf",
                    "ranking_municipios",
                ],
            },
            {
                "dimension": "dim_municipio",
                "key": "codigo_municipio_ibge",
                "targets": [
                    "ranking_municipios",
                ],
            },
        ],
    }

    manifest_path = destination / "dimensions.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Dimensões de emendas concluídas: %s",
        manifest_path,
    )

    return manifest_path
