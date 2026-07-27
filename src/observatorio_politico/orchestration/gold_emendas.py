from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_emendas import (
    build_ranking_favorecidos,
    build_ranking_funcoes,
    build_ranking_municipios,
    build_ranking_parlamentares,
    build_ranking_uf,
    build_relacionamento_autor_favorecido,
)

logger = logging.getLogger(__name__)


def _write_dataset(
    *,
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
        "Gold criada: dataset=%s registros=%s",
        name,
        dataframe.height,
    )

    return {
        "dataset": name,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_gold_emendas(
    *,
    years: list[int],
) -> Path:
    years_label = "_".join(str(year) for year in years)

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "historico_emendas"
        / f"anos={years_label}"
    )

    emendas_path = silver_root / "emendas" / "emendas.parquet"
    favorecidos_path = silver_root / "favorecidos" / "favorecidos.parquet"

    if not emendas_path.exists():
        raise FileNotFoundError(f"Silver de emendas não encontrada: {emendas_path}")

    if not favorecidos_path.exists():
        raise FileNotFoundError(
            f"Silver de favorecidos não encontrada: {favorecidos_path}"
        )

    logger.info(
        "Lendo Silver: emendas=%s",
        emendas_path,
    )
    emendas = pl.read_parquet(emendas_path)

    logger.info(
        "Lendo Silver: favorecidos=%s",
        favorecidos_path,
    )
    favorecidos = pl.read_parquet(favorecidos_path)

    gold_root = (
        Path("data/gold") / "portal_transparencia" / "emendas" / f"anos={years_label}"
    )

    datasets = {
        "ranking_parlamentares": (
            build_ranking_parlamentares(
                emendas,
                favorecidos,
            )
        ),
        "ranking_favorecidos": (
            build_ranking_favorecidos(
                favorecidos,
            )
        ),
        "ranking_uf": build_ranking_uf(emendas),
        "ranking_municipios": (build_ranking_municipios(emendas)),
        "ranking_funcoes": (build_ranking_funcoes(emendas)),
        "relacionamento_autor_favorecido": (
            build_relacionamento_autor_favorecido(favorecidos)
        ),
    }

    outputs: list[dict[str, object]] = []

    for name, dataframe in datasets.items():
        output = _write_dataset(
            dataframe=dataframe,
            destination=gold_root / name,
            name=name,
        )
        outputs.append(output)

    execution_time = datetime.now(UTC)

    manifest = {
        "source": "portal_transparencia",
        "layer": "gold",
        "subject": "emendas",
        "years": years,
        "processed_at_utc": (execution_time.isoformat()),
        "source_record_counts": {
            "emendas": emendas.height,
            "favorecidos": favorecidos.height,
        },
        "datasets": outputs,
        "disclaimer": (
            "Os indicadores são descritivos. "
            "Valores elevados ou relacionamentos financeiros "
            "não representam, por si só, irregularidade."
        ),
    }

    manifest_path = gold_root / "gold.manifest.json"

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
        "Camada Gold concluída: %s",
        manifest_path,
    )

    return manifest_path
