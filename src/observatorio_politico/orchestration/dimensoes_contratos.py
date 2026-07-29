from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.dimensoes_contratos import (
    build_dim_contratado,
    build_dim_modalidade_compra,
    build_dim_orgao,
    build_dim_situacao_contrato,
    build_dim_tempo,
    build_dim_unidade_gestora,
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
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    logger.info(
        "Dimensão criada: dimensão=%s registros=%s",
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


def run_dimensions_contratos(
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

    gold_root = (
        Path("data/gold")
        / "portal_transparencia"
        / "contratos"
        / f"periodos={period_label}"
    )

    reconciliation_manifest_path = (
        gold_root / "reconciliation" / "reconciliation.manifest.json"
    )

    if not reconciliation_manifest_path.exists():
        raise FileNotFoundError(
            f"Manifesto de reconciliação não encontrado: {reconciliation_manifest_path}"
        )

    reconciliation = json.loads(
        reconciliation_manifest_path.read_text(encoding="utf-8")
    )

    if not reconciliation.get(
        "approved",
        False,
    ):
        raise ValueError("A reconciliação de contratos não está aprovada.")

    contratos_path = silver_root / "contratos_atuais" / "contratos_atuais.parquet"

    contratos = pl.read_parquet(contratos_path)

    dimensions = {
        "dim_orgao": build_dim_orgao(contratos),
        "dim_unidade_gestora": (build_dim_unidade_gestora(contratos)),
        "dim_contratado": (build_dim_contratado(contratos)),
        "dim_modalidade_compra": (build_dim_modalidade_compra(contratos)),
        "dim_situacao_contrato": (build_dim_situacao_contrato(contratos)),
        "dim_tempo": build_dim_tempo(
            first_period=first_period,
            last_period=last_period,
        ),
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
        "source": "portal_transparencia",
        "subject": "contratos_federais",
        "layer": "gold_dimensions",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_approved": True,
        "dimensions": outputs,
        "power_bi_model_notes": [
            ("dim_orgao relaciona-se por chave_orgao."),
            ("dim_unidade_gestora relaciona-se por chave_unidade_gestora."),
            ("dim_contratado relaciona-se por codigo_contratado."),
            (
                "dim_tempo pode ser usada nas datas "
                "de assinatura e vigência por meio "
                "de relacionamentos ativos e "
                "inativos no Power BI."
            ),
        ],
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
        "Dimensões de contratos concluídas: %s",
        manifest_path,
    )

    return manifest_path
