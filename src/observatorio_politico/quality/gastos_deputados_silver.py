from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _check(
    *,
    check: str,
    value: float,
    approved: bool,
    severity: str,
    message: str | None = None,
) -> dict[str, object]:
    return {
        "check": check,
        "value": value,
        "severity": severity,
        "approved": approved,
        "message": message,
    }


def run_quality_silver_gastos_deputados(
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

    source_path = silver_root / "despesas" / "despesas.parquet"

    if not source_path.exists():
        raise FileNotFoundError(f"Silver de gastos não encontrada: {source_path}")

    despesas = pl.read_parquet(source_path)

    duplicate_expense_keys = (
        despesas.group_by("chave_despesa").len().filter(pl.col("len") > 1).height
    )

    null_expense_keys = despesas["chave_despesa"].null_count()

    invalid_years = despesas.filter(
        ~pl.col("ano_despesa").is_in(normalized_years)
    ).height

    invalid_months = despesas.filter(
        ~pl.col("mes_despesa").is_between(
            1,
            12,
        )
    ).height

    negative_net_rows = despesas.filter(pl.col("valor_liquido") < 0).height

    negative_outside_airfare = despesas.filter(
        (pl.col("valor_liquido") < 0) & ~pl.col("codigo_tipo_despesa").is_in([998, 999])
    ).height

    institutional_rows = despesas.filter(
        pl.col("tipo_beneficiario") == "LIDERANCA_OU_ORGAO"
    ).height

    missing_issue_dates = despesas["data_emissao"].null_count()

    missing_supplier_documents = despesas["documento_fornecedor"].null_count()

    financially_inconsistent = despesas.filter(~pl.col("financeiro_consistente")).height

    invalid_movement_types = despesas.filter(
        ~pl.col("tipo_movimento").is_in(
            [
                "DESPESA",
                "ESTORNO_AEREO",
                "ESTORNO_OU_AJUSTE",
            ]
        )
    ).height

    source_count = despesas.height
    distinct_expense_count = despesas["chave_despesa"].n_unique()

    checks = [
        _check(
            check="dataset_not_empty",
            value=despesas.height,
            approved=despesas.height > 0,
            severity=("info" if despesas.height > 0 else "error"),
        ),
        _check(
            check="expense_key_not_null",
            value=null_expense_keys,
            approved=null_expense_keys == 0,
            severity=("info" if null_expense_keys == 0 else "error"),
        ),
        _check(
            check="expense_key_unique",
            value=duplicate_expense_keys,
            approved=duplicate_expense_keys == 0,
            severity=("info" if duplicate_expense_keys == 0 else "error"),
        ),
        _check(
            check="record_count_matches_unique_keys",
            value=source_count - distinct_expense_count,
            approved=(source_count == distinct_expense_count),
            severity=("info" if source_count == distinct_expense_count else "error"),
        ),
        _check(
            check="expense_year_valid",
            value=invalid_years,
            approved=invalid_years == 0,
            severity=("info" if invalid_years == 0 else "error"),
        ),
        _check(
            check="expense_month_valid",
            value=invalid_months,
            approved=invalid_months == 0,
            severity=("info" if invalid_months == 0 else "error"),
        ),
        _check(
            check="movement_type_valid",
            value=invalid_movement_types,
            approved=invalid_movement_types == 0,
            severity=("info" if invalid_movement_types == 0 else "error"),
        ),
        _check(
            check="negative_net_rows",
            value=negative_net_rows,
            approved=True,
            severity=("warning" if negative_net_rows > 0 else "info"),
            message=("Valores negativos foram preservados como estornos ou ajustes."),
        ),
        _check(
            check="negative_outside_airfare",
            value=negative_outside_airfare,
            approved=True,
            severity=("warning" if negative_outside_airfare > 0 else "info"),
            message=(
                "Movimentos negativos fora das subcotas 998 e 999 devem ser auditados."
            ),
        ),
        _check(
            check="institutional_beneficiaries",
            value=institutional_rows,
            approved=True,
            severity=("warning" if institutional_rows > 0 else "info"),
            message=(
                "Lideranças partidárias e órgãos foram preservados separadamente."
            ),
        ),
        _check(
            check="missing_issue_dates",
            value=missing_issue_dates,
            approved=True,
            severity=("warning" if missing_issue_dates > 0 else "info"),
        ),
        _check(
            check="missing_supplier_documents",
            value=missing_supplier_documents,
            approved=True,
            severity=("warning" if missing_supplier_documents > 0 else "info"),
        ),
        _check(
            check="financially_inconsistent_rows",
            value=financially_inconsistent,
            approved=True,
            severity=("warning" if financially_inconsistent > 0 else "info"),
            message=(
                "Registros com diferença entre documento, "
                "glosa e líquido foram preservados."
            ),
        ),
    ]

    quality = pl.DataFrame(
        checks,
        infer_schema_length=None,
    )

    approved = bool(quality["approved"].all())

    destination = silver_root / "quality"

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "validacao_silver_gastos_deputados.parquet"

    csv_path = destination / "validacao_silver_gastos_deputados.csv"

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
        "source": "camara_deputados",
        "subject": "gastos_deputados_ceap",
        "layer": "silver_quality",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "record_count": despesas.height,
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
        "Qualidade Silver de gastos concluída: verificações=%s alertas=%s aprovado=%s",
        quality.height,
        len(warnings),
        approved,
    )

    return manifest_path
