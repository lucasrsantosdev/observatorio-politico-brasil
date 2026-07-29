from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.dimensoes_licitacoes import (
    build_dim_fornecedor,
    build_dim_localidade,
    build_dim_modalidade,
    build_dim_orgao,
    build_dim_situacao,
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
        "Dimensão de licitações criada: dimensão=%s registros=%s",
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


def run_dimensions_licitacoes(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    period_label = f"{first_period}_{last_period}"

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    gold_root = (
        Path("data/gold")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    reconciliation_path = gold_root / "reconciliation" / "reconciliation.manifest.json"

    if not reconciliation_path.exists():
        raise FileNotFoundError(
            f"Manifesto de reconciliação não encontrado: {reconciliation_path}"
        )

    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))

    if not reconciliation.get(
        "approved",
        False,
    ):
        raise ValueError("A reconciliação de licitações não está aprovada.")

    licitacoes = pl.read_parquet(silver_root / "licitacoes" / "licitacoes.parquet")

    itens = pl.read_parquet(silver_root / "itens_licitacao" / "itens_licitacao.parquet")

    dimensions = {
        "dim_orgao": (build_dim_orgao(licitacoes)),
        "dim_unidade_gestora": (build_dim_unidade_gestora(licitacoes)),
        "dim_fornecedor": (build_dim_fornecedor(itens)),
        "dim_modalidade_compra": (build_dim_modalidade(licitacoes)),
        "dim_situacao_licitacao": (build_dim_situacao(licitacoes)),
        "dim_localidade": (build_dim_localidade(licitacoes)),
        "dim_tempo": (build_dim_tempo(licitacoes)),
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
        "subject": "licitacoes",
        "layer": "gold_dimensions",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_approved": True,
        "dimensions": outputs,
        "power_bi_model_notes": [
            ("concorrencia_licitacoes é a tabela fato principal de licitações."),
            (
                "dim_fornecedor relaciona-se aos rankings "
                "e relacionamentos pelo código do vencedor."
            ),
            (
                "dim_tempo pode ser relacionada com "
                "data_abertura e data_resultado_compra."
            ),
            (
                "valor_total_item_calculado deve ser tratado "
                "como campo analítico e não como valor "
                "financeiro oficial reconciliado."
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
        "Dimensões de licitações concluídas: %s",
        manifest_path,
    )

    return manifest_path
