from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_gastos_deputados import (
    build_fato_gastos_deputados,
    build_ranking_deputados,
    build_ranking_fornecedores,
    build_ranking_partidos,
    build_ranking_tipos_despesa,
    build_ranking_ufs,
    build_resumo_mensal,
)

logger = logging.getLogger(__name__)


def _write_dataset(
    *,
    dataframe: pl.DataFrame,
    destination_root: Path,
    dataset: str,
) -> dict[str, object]:
    destination = destination_root / dataset

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dataset}.parquet"
    csv_path = destination / f"{dataset}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    logger.info(
        "Gold de gastos criada: dataset=%s registros=%s",
        dataset,
        dataframe.height,
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_gold_gastos_deputados(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    years_label = "_".join(str(year) for year in normalized_years)

    silver_root = (
        Path("data/silver")
        / "camara_deputados"
        / "gastos_deputados"
        / f"anos={years_label}"
    )

    quality_manifest_path = silver_root / "quality" / "quality.manifest.json"

    if not quality_manifest_path.exists():
        raise FileNotFoundError(
            f"Manifesto de qualidade não encontrado: {quality_manifest_path}"
        )

    quality_manifest = json.loads(quality_manifest_path.read_text(encoding="utf-8"))

    if not quality_manifest.get(
        "approved",
        False,
    ):
        raise ValueError("A Silver de gastos dos deputados não está aprovada.")

    source_path = silver_root / "despesas" / "despesas.parquet"

    despesas = pl.read_parquet(source_path)

    fato = build_fato_gastos_deputados(despesas)

    datasets = {
        "fato_gastos_deputados": fato,
        "ranking_deputados": (build_ranking_deputados(fato)),
        "ranking_partidos": (build_ranking_partidos(fato)),
        "ranking_ufs": (build_ranking_ufs(fato)),
        "ranking_fornecedores": (build_ranking_fornecedores(fato)),
        "ranking_tipos_despesa": (build_ranking_tipos_despesa(fato)),
        "resumo_mensal": (build_resumo_mensal(fato)),
    }

    destination_root = (
        Path("data/gold")
        / "camara_deputados"
        / "gastos_deputados"
        / f"anos={years_label}"
    )

    outputs = [
        _write_dataset(
            dataframe=dataframe,
            destination_root=destination_root,
            dataset=dataset,
        )
        for dataset, dataframe in datasets.items()
    ]

    manifest = {
        "source": "camara_deputados",
        "subject": "gastos_deputados_ceap",
        "layer": "gold",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "quality_approved": True,
        "datasets": outputs,
        "methodology_notes": [
            ("fato_gastos_deputados preserva todas as linhas físicas da Silver."),
            (
                "ranking_deputados inclui somente "
                "beneficiários classificados como parlamentares."
            ),
            ("Lideranças partidárias e órgãos permanecem disponíveis na tabela fato."),
            ("Valores negativos são preservados como estornos ou ajustes."),
            (
                "valor_liquido_apos_restituicao é um campo "
                "analítico calculado e não substitui o valor "
                "líquido oficial da fonte."
            ),
        ],
    }

    manifest_path = destination_root / "gold.manifest.json"

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
        "Gold de gastos dos deputados concluída: %s",
        manifest_path,
    )

    return manifest_path
