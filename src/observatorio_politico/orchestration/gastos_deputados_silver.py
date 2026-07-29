from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gastos_deputados import (
    transform_gastos_deputados,
)

logger = logging.getLogger(__name__)


def run_silver_gastos_deputados(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    frames: list[pl.DataFrame] = []
    sources: list[dict[str, object]] = []

    bronze_root = Path("data/bronze") / "camara_deputados" / "gastos_deputados"

    for year in normalized_years:
        year_root = bronze_root / f"ano={year}"

        csv_files = sorted(year_root.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(
                f"Nenhum CSV Bronze encontrado para o ano {year}: {year_root}"
            )

        for csv_path in csv_files:
            frame = (
                pl.read_csv(
                    csv_path,
                    separator=";",
                    encoding="utf8-lossy",
                    infer_schema_length=10000,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                )
                .with_row_index(
                    name="linha_arquivo",
                    offset=2,
                )
                .with_columns(
                    pl.lit(year).cast(pl.Int32).alias("ano_arquivo_origem"),
                    pl.lit(csv_path.name).alias("arquivo_origem"),
                )
            )

            frames.append(frame)

            sources.append(
                {
                    "year": year,
                    "file": str(csv_path),
                    "record_count": (frame.height),
                }
            )

    raw = pl.concat(
        frames,
        how="vertical_relaxed",
    )

    silver = transform_gastos_deputados(raw)

    years_label = "_".join(str(year) for year in normalized_years)

    destination = (
        Path("data/silver")
        / "camara_deputados"
        / "gastos_deputados"
        / f"anos={years_label}"
        / "despesas"
    )

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "despesas.parquet"

    csv_path = destination / "despesas.csv"

    silver.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    silver.write_csv(
        csv_path,
        separator=";",
    )

    manifest = {
        "source": "camara_deputados",
        "subject": "gastos_deputados_ceap",
        "layer": "silver",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "source_record_count": raw.height,
        "record_count": silver.height,
        "column_count": silver.width,
        "distinct_expense_keys": silver["chave_despesa"].n_unique(),
        "distinct_document_keys": silver["chave_documento"].n_unique(),
        "distinct_beneficiaries": silver["chave_beneficiario"].n_unique(),
        "distinct_suppliers": silver["chave_fornecedor"].n_unique(),
        "total_document_value": float(silver["valor_documento"].sum() or 0.0),
        "total_glosa_value": float(silver["valor_glosa"].sum() or 0.0),
        "total_net_value": float(silver["valor_liquido"].sum() or 0.0),
        "negative_net_rows": (silver.filter(pl.col("valor_liquido") < 0).height),
        "financially_inconsistent_rows": (
            silver.filter(~pl.col("financeiro_consistente")).height
        ),
        "sources": sources,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
        "methodology_notes": [
            ("Cada linha física da fonte é preservada na Silver."),
            ("ideDocumento não é utilizado como chave física única."),
            (
                "Valores negativos das subcotas 998 "
                "e 999 são classificados como "
                "estornos aéreos."
            ),
            (
                "Registros de lideranças partidárias "
                "são preservados e classificados "
                "separadamente."
            ),
        ],
    }

    manifest_path = destination.parent / "silver.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Silver de gastos dos deputados concluída: registros=%s colunas=%s",
        silver.height,
        silver.width,
    )

    return manifest_path
