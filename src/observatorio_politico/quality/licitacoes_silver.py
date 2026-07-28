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
    nulls = dataframe[key].null_count()
    distinct = dataframe[key].n_unique()
    duplicates = dataframe.height - distinct

    approved = duplicates == 0 and nulls == 0

    return {
        "check": "unique_key",
        "dataset": dataset,
        "key": key,
        "rows": dataframe.height,
        "distinct_keys": distinct,
        "duplicates": duplicates,
        "null_keys": nulls,
        "severity": ("info" if approved else "error"),
        "approved": approved,
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

    effective_severity = severity if missing_keys > 0 else "info"

    approved = missing_keys == 0 or severity == "warning"

    return {
        "check": "referential_integrity",
        "source_dataset": source_dataset,
        "target_dataset": target_dataset,
        "source_key": source_key,
        "target_key": target_key,
        "missing_keys": missing_keys,
        "severity": effective_severity,
        "approved": approved,
    }


def _check_negative_values(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
    columns: list[str],
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for column in columns:
        negative_rows = dataframe.filter(
            pl.col(column).is_not_null() & (pl.col(column) < 0)
        ).height

        approved = negative_rows == 0

        results.append(
            {
                "check": "negative_values",
                "dataset": dataset,
                "column": column,
                "negative_rows": negative_rows,
                "severity": ("info" if approved else "error"),
                "approved": approved,
            }
        )

    return results


def _check_dates(
    licitacoes: pl.DataFrame,
) -> list[dict[str, object]]:
    result_before_opening = licitacoes.filter(
        pl.col("data_resultado_compra").is_not_null()
        & pl.col("data_abertura").is_not_null()
        & (pl.col("data_resultado_compra") < pl.col("data_abertura"))
    ).height

    negative_duration = licitacoes.filter(
        pl.col("duracao_processo_dias").is_not_null()
        & (pl.col("duracao_processo_dias") < 0)
    ).height

    return [
        {
            "check": "result_before_opening",
            "dataset": "licitacoes",
            "rows": result_before_opening,
            "severity": ("warning" if result_before_opening > 0 else "info"),
            "approved": True,
        },
        {
            "check": "negative_process_duration",
            "dataset": "licitacoes",
            "rows": negative_duration,
            "severity": ("warning" if negative_duration > 0 else "info"),
            "approved": True,
        },
    ]


def _check_winners(
    participantes: pl.DataFrame,
) -> dict[str, object]:
    winners_without_identification = participantes.filter(
        pl.col("participante_vencedor")
        & ~pl.col("participante_identificado")
        & ~pl.col("participante_sigiloso")
    ).height

    approved = winners_without_identification == 0

    return {
        "check": ("winner_without_identification"),
        "dataset": ("participantes_licitacao"),
        "rows": (winners_without_identification),
        "severity": ("info" if approved else "error"),
        "approved": approved,
    }


def _check_orphan_participants_detail(
    participantes: pl.DataFrame,
    itens: pl.DataFrame,
) -> dict[str, object]:
    orphan_rows = participantes.join(
        itens.select("chave_item_licitacao").unique(),
        on="chave_item_licitacao",
        how="anti",
    )

    orphan_keys = orphan_rows["chave_item_licitacao"].n_unique()

    orphan_winners = orphan_rows.filter(pl.col("participante_vencedor")).height

    orphan_non_winners = orphan_rows.filter(~pl.col("participante_vencedor")).height

    item_code_zero = orphan_rows.filter(pl.col("codigo_item_compra") == "0").height

    confidential_participants = orphan_rows.filter(
        pl.col("codigo_participante") == "-11"
    ).height

    return {
        "check": ("orphan_participants_detail"),
        "dataset": ("participantes_licitacao"),
        "rows": orphan_rows.height,
        "missing_keys": orphan_keys,
        "orphan_winners": orphan_winners,
        "orphan_non_winners": (orphan_non_winners),
        "item_code_zero": item_code_zero,
        "confidential_participants": (confidential_participants),
        "severity": ("warning" if orphan_rows.height > 0 else "info"),
        "approved": True,
    }


def run_quality_silver_licitacoes(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    root = (
        Path("data/silver")
        / "portal_transparencia"
        / "licitacoes"
        / (f"periodos={first_period}_{last_period}")
    )

    licitacoes = _load_dataset(
        root,
        "licitacoes",
    )

    itens = _load_dataset(
        root,
        "itens_licitacao",
    )

    participantes = _load_dataset(
        root,
        "participantes_licitacao",
    )

    empenhos = _load_dataset(
        root,
        "empenhos_relacionados",
    )

    checks: list[dict[str, object]] = []

    checks.extend(
        [
            _check_unique_key(
                licitacoes,
                dataset="licitacoes",
                key="chave_licitacao",
            ),
            _check_unique_key(
                itens,
                dataset="itens_licitacao",
                key="chave_item_vencedor",
            ),
            _check_unique_key(
                participantes,
                dataset=("participantes_licitacao"),
                key="chave_participacao",
            ),
            _check_unique_key(
                empenhos,
                dataset=("empenhos_relacionados"),
                key=("chave_empenho_licitacao"),
            ),
        ]
    )

    checks.extend(
        [
            _check_reference(
                itens,
                licitacoes,
                source_dataset=("itens_licitacao"),
                target_dataset="licitacoes",
                source_key="chave_licitacao",
                target_key="chave_licitacao",
            ),
            _check_reference(
                participantes,
                licitacoes,
                source_dataset=("participantes_licitacao"),
                target_dataset="licitacoes",
                source_key="chave_licitacao",
                target_key="chave_licitacao",
            ),
            _check_reference(
                participantes,
                itens,
                source_dataset=("participantes_licitacao"),
                target_dataset=("itens_licitacao"),
                source_key=("chave_item_licitacao"),
                target_key=("chave_item_licitacao"),
                severity="warning",
            ),
            _check_reference(
                empenhos,
                licitacoes,
                source_dataset=("empenhos_relacionados"),
                target_dataset="licitacoes",
                source_key="chave_licitacao",
                target_key="chave_licitacao",
            ),
        ]
    )

    checks.extend(
        _check_negative_values(
            licitacoes,
            dataset="licitacoes",
            columns=[
                "valor_licitacao",
            ],
        )
    )

    checks.extend(
        _check_negative_values(
            itens,
            dataset="itens_licitacao",
            columns=[
                "quantidade_item",
                "valor_item",
                ("valor_total_item_calculado"),
            ],
        )
    )

    checks.extend(
        _check_negative_values(
            empenhos,
            dataset=("empenhos_relacionados"),
            columns=[
                "valor_empenho_r",
            ],
        )
    )

    checks.extend(_check_dates(licitacoes))

    checks.append(_check_winners(participantes))

    checks.append(
        _check_orphan_participants_detail(
            participantes,
            itens,
        )
    )

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

    parquet_path = destination / ("validacao_silver_licitacoes.parquet")

    csv_path = destination / ("validacao_silver_licitacoes.csv")

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
        "subject": ("licitacoes_federais"),
        "layer": "silver",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": (datetime.now(UTC).isoformat()),
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
        "Validação Silver de licitações "
        "concluída: verificações=%s "
        "alertas=%s aprovado=%s",
        quality.height,
        len(warning_checks),
        approved,
    )

    return manifest_path
