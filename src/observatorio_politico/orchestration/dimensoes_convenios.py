from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.dimensoes_convenios import (
    build_dim_convenente,
    build_dim_emenda,
    build_dim_funcao,
    build_dim_localidade,
    build_dim_subfuncao,
    build_dim_tempo_convenios,
    build_dim_tipo_emenda,
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
        "Dimensão de convênios criada: dimensão=%s registros=%s",
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


def run_dimensions_convenios(
    *,
    years: list[int],
) -> Path:
    years_label = "_".join(str(year) for year in sorted(years))

    gold_root = (
        Path("data/gold") / "portal_transparencia" / "convenios" / f"anos={years_label}"
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
        raise ValueError("A reconciliação de convênios não está aprovada.")

    fato = pl.read_parquet(gold_root / "fato_convenios" / "fato_convenios.parquet")

    relacionamento = pl.read_parquet(
        gold_root
        / "relacionamento_emenda_convenio"
        / "relacionamento_emenda_convenio.parquet"
    )

    dimensions = {
        "dim_convenente": (build_dim_convenente(fato)),
        "dim_funcao": (build_dim_funcao(fato)),
        "dim_subfuncao": (build_dim_subfuncao(fato)),
        "dim_localidade": (build_dim_localidade(fato)),
        "dim_tipo_emenda": (build_dim_tipo_emenda(fato)),
        "dim_emenda": (build_dim_emenda(relacionamento)),
        "dim_tempo": (build_dim_tempo_convenios(fato)),
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
        "subject": "emendas_convenios",
        "layer": "gold_dimensions",
        "years": sorted(years),
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "reconciliation_approved": True,
        "dimensions": outputs,
        "power_bi_model_notes": [
            ("fato_convenios relaciona-se com dim_convenente pelo nome do convenente."),
            (
                "relacionamento_emenda_convenio "
                "funciona como tabela ponte entre "
                "dim_emenda e fato_convenios."
            ),
            ("dim_tempo relaciona-se com data_publicacao_convenio."),
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
        "Dimensões de convênios concluídas: %s",
        manifest_path,
    )

    return manifest_path
