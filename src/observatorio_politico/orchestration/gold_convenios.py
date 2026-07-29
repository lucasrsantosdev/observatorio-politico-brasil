from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_convenios import (
    build_fato_convenios,
    build_ranking_convenentes,
    build_ranking_funcoes,
    build_ranking_localidades,
    build_relacionamento_emenda_convenio,
    build_resumo_anual,
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
        "Gold de convênios criada: dataset=%s registros=%s",
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


def run_gold_convenios(
    *,
    years: list[int],
) -> Path:
    years_label = "_".join(str(year) for year in sorted(years))

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "historico_emendas"
        / f"anos={years_label}"
    )

    quality_manifest_path = (
        silver_root / "convenios" / "quality" / "quality.manifest.json"
    )

    if not quality_manifest_path.exists():
        raise FileNotFoundError(
            "Manifesto de qualidade de convênios "
            f"não encontrado: {quality_manifest_path}"
        )

    quality = json.loads(quality_manifest_path.read_text(encoding="utf-8"))

    if not quality.get("approved", False):
        raise ValueError("A Silver de convênios não foi aprovada.")

    source_path = silver_root / "convenios" / "convenios.parquet"

    convenios = pl.read_parquet(source_path)

    fato = build_fato_convenios(convenios)

    datasets = {
        "fato_convenios": fato,
        "relacionamento_emenda_convenio": (
            build_relacionamento_emenda_convenio(convenios)
        ),
        "ranking_convenentes": (build_ranking_convenentes(fato)),
        "ranking_funcoes": (build_ranking_funcoes(fato)),
        "ranking_localidades": (build_ranking_localidades(fato)),
        "resumo_anual": (build_resumo_anual(fato)),
    }

    destination_root = (
        Path("data/gold") / "portal_transparencia" / "convenios" / f"anos={years_label}"
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
        "source": "portal_transparencia",
        "subject": "emendas_convenios",
        "layer": "gold",
        "years": sorted(years),
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "quality_approved": True,
        "datasets": outputs,
        "methodology_notes": [
            ("fato_convenios possui uma linha por número de convênio."),
            (
                "relacionamento_emenda_convenio "
                "preserva todas as relações entre "
                "emendas e convênios."
            ),
            (
                "O valor do convênio é somado apenas "
                "no grão físico do convênio para "
                "evitar dupla contagem."
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
        "Gold de convênios concluída: %s",
        manifest_path,
    )

    return manifest_path
