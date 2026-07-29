from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _load_dataset(
    root: Path,
    entity: str,
) -> pl.DataFrame:
    path = root / entity / f"{entity}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset Silver não encontrado: {path}")

    return pl.read_parquet(path)


def _check_unique_key(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
    key: str,
) -> dict[str, object]:
    if key not in dataframe.columns:
        return {
            "check": "unique_key",
            "dataset": dataset,
            "key": key,
            "rows": dataframe.height,
            "distinct_keys": None,
            "duplicates": None,
            "null_keys": None,
            "severity": "error",
            "approved": False,
            "message": (f"Coluna de chave não encontrada: {key}"),
        }

    null_keys = dataframe[key].null_count()
    distinct_keys = dataframe[key].n_unique()
    duplicates = dataframe.height - distinct_keys

    approved = null_keys == 0 and duplicates == 0

    return {
        "check": "unique_key",
        "dataset": dataset,
        "key": key,
        "rows": dataframe.height,
        "distinct_keys": distinct_keys,
        "duplicates": duplicates,
        "null_keys": null_keys,
        "severity": ("info" if approved else "error"),
        "approved": approved,
        "message": None,
    }


def _check_reference(
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    source_dataset: str,
    target_dataset: str,
    source_key: str,
    target_key: str,
    severity: str = "error",
) -> dict[str, object]:
    missing = (
        source.select(source_key)
        .filter(pl.col(source_key).is_not_null())
        .unique()
        .join(
            target.select(target_key).unique(),
            left_on=source_key,
            right_on=target_key,
            how="anti",
        )
    )

    missing_keys = missing.height

    approved = missing_keys == 0 or severity == "warning"

    return {
        "check": "referential_integrity",
        "source_dataset": source_dataset,
        "target_dataset": target_dataset,
        "source_key": source_key,
        "target_key": target_key,
        "missing_keys": missing_keys,
        "severity": (severity if missing_keys > 0 else "info"),
        "approved": approved,
    }


def _check_negative_values(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
    columns: list[str],
    severity: str = "error",
) -> list[dict[str, object]]:
    checks: list[dict[str, object]] = []

    for column in columns:
        if column not in dataframe.columns:
            checks.append(
                {
                    "check": "negative_values",
                    "dataset": dataset,
                    "column": column,
                    "negative_rows": None,
                    "severity": "error",
                    "approved": False,
                    "message": (f"Coluna não encontrada: {column}"),
                }
            )
            continue

        negative_rows = dataframe.filter(
            pl.col(column).is_not_null() & (pl.col(column) < 0)
        ).height

        approved = negative_rows == 0 or severity == "warning"

        checks.append(
            {
                "check": "negative_values",
                "dataset": dataset,
                "column": column,
                "negative_rows": negative_rows,
                "severity": (severity if negative_rows > 0 else "info"),
                "approved": approved,
                "message": (
                    "Valor negativo preservado conforme publicado pela fonte."
                    if negative_rows > 0
                    else None
                ),
            }
        )

    return checks


def _check_contract_dates(
    contratos: pl.DataFrame,
) -> list[dict[str, object]]:
    signature_after_start = contratos.filter(
        pl.col("data_assinatura_contrato").is_not_null()
        & pl.col("data_inicio_vigencia").is_not_null()
        & (pl.col("data_assinatura_contrato") > pl.col("data_inicio_vigencia"))
    ).height

    end_before_start = contratos.filter(
        pl.col("data_inicio_vigencia").is_not_null()
        & pl.col("data_fim_vigencia").is_not_null()
        & (pl.col("data_fim_vigencia") < pl.col("data_inicio_vigencia"))
    ).height

    negative_duration = contratos.filter(
        pl.col("duracao_vigencia_dias").is_not_null()
        & (pl.col("duracao_vigencia_dias") < 0)
    ).height

    return [
        {
            "check": "signature_after_start",
            "dataset": "contratos_historico",
            "rows": signature_after_start,
            "severity": ("warning" if signature_after_start > 0 else "info"),
            "approved": True,
        },
        {
            "check": "end_before_start",
            "dataset": "contratos_historico",
            "rows": end_before_start,
            "severity": ("warning" if end_before_start > 0 else "info"),
            "approved": True,
        },
        {
            "check": "negative_contract_duration",
            "dataset": "contratos_historico",
            "rows": negative_duration,
            "severity": ("warning" if negative_duration > 0 else "info"),
            "approved": True,
        },
    ]


def _check_current_contracts(
    contratos_atuais: pl.DataFrame,
) -> list[dict[str, object]]:
    distinct_groups = contratos_atuais["chave_grupo_contrato"].n_unique()

    multiple_record_groups = (
        contratos_atuais.group_by("chave_grupo_contrato")
        .len()
        .filter(pl.col("len") > 1)
        .height
    )

    invalid_current_period = contratos_atuais.filter(
        pl.col("periodo_origem") != pl.col("periodo_mais_recente")
    ).height

    return [
        {
            "check": "current_contract_groups",
            "dataset": "contratos_atuais",
            "rows": contratos_atuais.height,
            "distinct_keys": distinct_groups,
            "multiple_record_groups": (multiple_record_groups),
            "severity": ("warning" if multiple_record_groups > 0 else "info"),
            "approved": True,
        },
        {
            "check": "current_period_consistency",
            "dataset": "contratos_atuais",
            "rows": invalid_current_period,
            "severity": ("error" if invalid_current_period > 0 else "info"),
            "approved": (invalid_current_period == 0),
        },
    ]


def _check_special_items(
    itens: pl.DataFrame,
) -> dict[str, object]:
    item_code_zero = itens.filter(pl.col("item_codigo_zero")).height

    confidential_items = itens.filter(pl.col("item_sigiloso")).height

    return {
        "check": "special_contract_items",
        "dataset": "itens_contrato",
        "rows": itens.height,
        "item_code_zero": item_code_zero,
        "confidential_items": confidential_items,
        "severity": (
            "warning" if (item_code_zero > 0 or confidential_items > 0) else "info"
        ),
        "approved": True,
    }


def _check_apostilamentos(
    apostilamentos: pl.DataFrame,
) -> dict[str, object]:
    approved = apostilamentos.width > 0

    return {
        "check": "apostilamentos_availability",
        "dataset": "apostilamentos",
        "rows": apostilamentos.height,
        "column_count": apostilamentos.width,
        "severity": (
            "warning"
            if approved and apostilamentos.height == 0
            else ("info" if approved else "error")
        ),
        "approved": approved,
    }


def run_quality_silver_contratos(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    root = (
        Path("data/silver")
        / "portal_transparencia"
        / "contratos"
        / (f"periodos={first_period}_{last_period}")
    )

    contratos_historico = _load_dataset(
        root,
        "contratos_historico",
    )

    contratos_atuais = _load_dataset(
        root,
        "contratos_atuais",
    )

    itens = _load_dataset(
        root,
        "itens_contrato",
    )

    termos = _load_dataset(
        root,
        "termos_aditivos",
    )

    apostilamentos = _load_dataset(
        root,
        "apostilamentos",
    )

    checks: list[dict[str, object]] = [
        _check_unique_key(
            contratos_historico,
            dataset="contratos_historico",
            key="chave_registro_contrato",
        ),
        _check_unique_key(
            contratos_atuais,
            dataset="contratos_atuais",
            key="chave_registro_contrato",
        ),
        _check_unique_key(
            itens,
            dataset="itens_contrato",
            key="chave_item_contrato",
        ),
        _check_unique_key(
            termos,
            dataset="termos_aditivos",
            key="chave_termo_aditivo",
        ),
        _check_reference(
            contratos_atuais,
            contratos_historico,
            source_dataset="contratos_atuais",
            target_dataset="contratos_historico",
            source_key="chave_registro_contrato",
            target_key="chave_registro_contrato",
        ),
        _check_reference(
            itens,
            contratos_historico,
            source_dataset="itens_contrato",
            target_dataset="contratos_historico",
            source_key="chave_grupo_contrato",
            target_key="chave_grupo_contrato",
        ),
        _check_reference(
            termos,
            contratos_historico,
            source_dataset="termos_aditivos",
            target_dataset="contratos_historico",
            source_key="chave_grupo_contrato",
            target_key="chave_grupo_contrato",
        ),
    ]

    checks.extend(
        _check_negative_values(
            contratos_historico,
            dataset="contratos_historico",
            columns=[
                "valor_inicial_compra",
                "valor_final_compra",
            ],
        )
    )

    checks.extend(
        _check_negative_values(
            itens,
            dataset="itens_contrato",
            columns=[
                "quantidade_item",
                "valor_item",
                "valor_total_item_calculado",
            ],
            severity="warning",
        )
    )

    checks.extend(_check_contract_dates(contratos_historico))

    checks.extend(_check_current_contracts(contratos_atuais))

    checks.append(_check_special_items(itens))

    checks.append(_check_apostilamentos(apostilamentos))

    quality = pl.DataFrame(
        checks,
        infer_schema_length=None,
    )

    approved = bool(quality["approved"].all())

    destination = root / "quality"

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "validacao_silver_contratos.parquet"

    csv_path = destination / "validacao_silver_contratos.csv"

    quality.write_parquet(
        parquet_path,
        compression="zstd",
    )

    quality.write_csv(
        csv_path,
        separator=";",
    )

    failed_checks = quality.filter(~pl.col("approved")).to_dicts()

    warning_checks = quality.filter(pl.col("severity") == "warning").to_dicts()

    manifest = {
        "source": "portal_transparencia",
        "subject": "contratos_federais",
        "layer": "silver",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": quality.height,
        "approved": approved,
        "failed_checks": failed_checks,
        "warning_count": len(warning_checks),
        "warning_checks": warning_checks,
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
        "Validação Silver de contratos concluída: "
        "verificações=%s alertas=%s aprovado=%s",
        quality.height,
        len(warning_checks),
        approved,
    )

    return manifest_path
