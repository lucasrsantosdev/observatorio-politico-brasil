from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_contratos import (
    build_contratos_atuais_resumo,
    build_contratos_com_variacao,
    build_itens_contrato_atuais,
    build_ranking_contratados,
    build_ranking_orgaos,
    build_relacionamento_orgao_contratado,
    build_termos_por_contrato,
)

logger = logging.getLogger(__name__)


def _read_silver(
    root: Path,
    entity: str,
) -> pl.DataFrame:
    path = root / entity / f"{entity}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Silver não encontrada: {path}")

    logger.info(
        "Lendo Silver: entidade=%s arquivo=%s",
        entity,
        path,
    )

    return pl.read_parquet(path)


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
        "Gold criada: dataset=%s registros=%s",
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


def run_gold_contratos(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    period_label = f"{first_period}_{last_period}"

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "contratos"
        / f"periodos={period_label}"
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
        raise ValueError("A Silver de contratos não está aprovada pela qualidade.")

    contratos_atuais = _read_silver(
        silver_root,
        "contratos_atuais",
    )

    itens = _read_silver(
        silver_root,
        "itens_contrato",
    )

    termos = _read_silver(
        silver_root,
        "termos_aditivos",
    )

    itens_atuais = build_itens_contrato_atuais(itens)

    datasets = {
        "contratos_atuais_detalhe": (contratos_atuais),
        "itens_contrato_atuais": (itens_atuais),
        "contratos_atuais_resumo": (
            build_contratos_atuais_resumo(
                contratos_atuais,
                itens_atuais,
                termos,
            )
        ),
        "ranking_contratados": (build_ranking_contratados(contratos_atuais)),
        "ranking_orgaos": (build_ranking_orgaos(contratos_atuais)),
        "contratos_com_variacao": (build_contratos_com_variacao(contratos_atuais)),
        "termos_por_contrato": (
            build_termos_por_contrato(
                contratos_atuais,
                termos,
            )
        ),
        "relacionamento_orgao_contratado": (
            build_relacionamento_orgao_contratado(contratos_atuais)
        ),
    }

    destination_root = (
        Path("data/gold")
        / "portal_transparencia"
        / "contratos"
        / f"periodos={period_label}"
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
        "subject": "contratos_federais",
        "layer": "gold",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "silver_quality_approved": True,
        "silver_warning_count": (
            quality_manifest.get(
                "warning_count",
                0,
            )
        ),
        "datasets": outputs,
        "methodology_notes": [
            (
                "Os rankings são descritivos e "
                "não representam evidência de "
                "irregularidade."
            ),
            (
                "Código UG e número do contrato "
                "formam uma chave de agrupamento, "
                "não uma chave física única."
            ),
            (
                "Contratos atuais preservam todos "
                "os registros distintos do período "
                "mais recente de cada grupo."
            ),
            (
                "Valores negativos publicados pela "
                "fonte foram preservados e "
                "documentados na qualidade."
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
        "Gold de contratos concluída: %s",
        manifest_path,
    )

    return manifest_path
