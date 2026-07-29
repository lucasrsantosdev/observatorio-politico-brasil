from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def run_quality_silver_convenios(
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

    source_path = silver_root / "convenios" / "convenios.parquet"

    if not source_path.exists():
        raise FileNotFoundError(f"Silver de convênios não encontrada: {source_path}")

    convenios = pl.read_parquet(source_path)

    pair_columns = [
        "codigo_emenda",
        "numero_convenio",
    ]

    duplicate_pairs = convenios.group_by(pair_columns).len().filter(pl.col("len") > 1)

    exact_duplicates = (
        convenios.group_by(convenios.columns).len().filter(pl.col("len") > 1)
    )

    content_analysis = (
        convenios.group_by("numero_convenio")
        .agg(
            pl.col("convenente").n_unique().alias("quantidade_convenentes"),
            pl.col("objeto_convenio").n_unique().alias("quantidade_objetos"),
            pl.col("valor_convenio").n_unique().alias("quantidade_valores"),
            pl.col("data_publicacao_convenio").n_unique().alias("quantidade_datas"),
        )
        .filter(
            (pl.col("quantidade_convenentes") > 1)
            | (pl.col("quantidade_objetos") > 1)
            | (pl.col("quantidade_valores") > 1)
            | (pl.col("quantidade_datas") > 1)
        )
    )

    checks: list[dict[str, object]] = [
        {
            "check": "dataset_not_empty",
            "dataset": "convenios",
            "value": convenios.height,
            "severity": ("info" if convenios.height > 0 else "error"),
            "approved": convenios.height > 0,
            "message": None,
        },
        {
            "check": "codigo_emenda_not_null",
            "dataset": "convenios",
            "value": convenios["codigo_emenda"].null_count(),
            "severity": (
                "info" if convenios["codigo_emenda"].null_count() == 0 else "error"
            ),
            "approved": (convenios["codigo_emenda"].null_count() == 0),
            "message": None,
        },
        {
            "check": "numero_convenio_not_null",
            "dataset": "convenios",
            "value": convenios["numero_convenio"].null_count(),
            "severity": (
                "info" if convenios["numero_convenio"].null_count() == 0 else "error"
            ),
            "approved": (convenios["numero_convenio"].null_count() == 0),
            "message": None,
        },
        {
            "check": "emenda_convenio_unique",
            "dataset": "convenios",
            "value": duplicate_pairs.height,
            "severity": ("info" if duplicate_pairs.height == 0 else "error"),
            "approved": duplicate_pairs.height == 0,
            "message": (
                None
                if duplicate_pairs.height == 0
                else "Existem pares emenda-convênio repetidos."
            ),
        },
        {
            "check": "exact_duplicates",
            "dataset": "convenios",
            "value": exact_duplicates.height,
            "severity": ("info" if exact_duplicates.height == 0 else "error"),
            "approved": exact_duplicates.height == 0,
            "message": None,
        },
        {
            "check": "contract_content_consistency",
            "dataset": "convenios",
            "value": content_analysis.height,
            "severity": ("info" if content_analysis.height == 0 else "error"),
            "approved": content_analysis.height == 0,
            "message": (
                None
                if content_analysis.height == 0
                else (
                    "O mesmo número de convênio possui conteúdos físicos divergentes."
                )
            ),
        },
        {
            "check": "negative_values",
            "dataset": "convenios",
            "value": convenios.filter(pl.col("valor_convenio") < 0).height,
            "severity": (
                "warning"
                if convenios.filter(pl.col("valor_convenio") < 0).height > 0
                else "info"
            ),
            "approved": True,
            "message": (
                "Valores negativos foram preservados da fonte."
                if convenios.filter(pl.col("valor_convenio") < 0).height > 0
                else None
            ),
        },
        {
            "check": "missing_object",
            "dataset": "convenios",
            "value": convenios["objeto_convenio"].null_count(),
            "severity": (
                "warning" if convenios["objeto_convenio"].null_count() > 0 else "info"
            ),
            "approved": True,
            "message": (
                "Objeto não informado em parte dos registros."
                if convenios["objeto_convenio"].null_count() > 0
                else None
            ),
        },
        {
            "check": "multi_emenda_contracts",
            "dataset": "convenios",
            "value": (
                convenios.group_by("numero_convenio")
                .agg(pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"))
                .filter(pl.col("quantidade_emendas") > 1)
                .height
            ),
            "severity": "warning",
            "approved": True,
            "message": (
                "Convênios ligados a múltiplas emendas "
                "foram preservados no relacionamento."
            ),
        },
    ]

    quality = pl.DataFrame(
        checks,
        infer_schema_length=None,
    )

    approved = bool(quality["approved"].all())

    destination = silver_root / "convenios" / "quality"

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "validacao_silver_convenios.parquet"

    csv_path = destination / "validacao_silver_convenios.csv"

    quality.write_parquet(
        parquet_path,
        compression="zstd",
    )

    quality.write_csv(
        csv_path,
        separator=";",
    )

    warnings = quality.filter(pl.col("severity") == "warning").to_dicts()

    failures = quality.filter(~pl.col("approved")).to_dicts()

    manifest = {
        "source": "portal_transparencia",
        "subject": "emendas_convenios",
        "layer": "silver",
        "years": sorted(years),
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "record_count": convenios.height,
        "distinct_contracts": convenios["numero_convenio"].n_unique(),
        "distinct_amendments": convenios["codigo_emenda"].n_unique(),
        "total_value_relationship_grain": float(
            convenios["valor_convenio"].sum() or 0.0
        ),
        "check_count": quality.height,
        "approved": approved,
        "warning_count": len(warnings),
        "warning_checks": warnings,
        "failed_checks": failures,
        "quality_parquet": str(parquet_path),
        "quality_csv": str(csv_path),
    }

    manifest_path = destination / "quality.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Qualidade Silver de convênios concluída: "
        "verificações=%s alertas=%s aprovado=%s",
        quality.height,
        len(warnings),
        approved,
    )

    return manifest_path
