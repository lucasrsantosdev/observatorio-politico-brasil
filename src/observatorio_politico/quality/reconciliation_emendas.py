from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


TOLERANCE = 0.01


def _difference(
    source_value: float,
    target_value: float,
) -> float:
    return round(target_value - source_value, 2)


def _is_reconciled(difference: float) -> bool:
    return abs(difference) <= TOLERANCE


def run_reconciliation_emendas(
    *,
    years: list[int],
) -> Path:
    years_label = "_".join(str(year) for year in years)

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "historico_emendas"
        / f"anos={years_label}"
    )

    gold_root = (
        Path("data/gold") / "portal_transparencia" / "emendas" / f"anos={years_label}"
    )

    emendas = pl.read_parquet(silver_root / "emendas" / "emendas.parquet")

    favorecidos = pl.read_parquet(silver_root / "favorecidos" / "favorecidos.parquet")

    ranking_parlamentares = pl.read_parquet(
        gold_root / "ranking_parlamentares" / "ranking_parlamentares.parquet"
    )

    ranking_favorecidos = pl.read_parquet(
        gold_root / "ranking_favorecidos" / "ranking_favorecidos.parquet"
    )

    silver_emendas_totals = (
        emendas.group_by("ano_emenda")
        .agg(
            pl.col("valor_empenhado").sum(),
            pl.col("valor_liquidado").sum(),
            pl.col("valor_pago").sum(),
        )
        .sort("ano_emenda")
    )

    gold_parlamentares_totals = (
        ranking_parlamentares.group_by("ano_emenda")
        .agg(
            pl.col("valor_empenhado").sum(),
            pl.col("valor_liquidado").sum(),
            pl.col("valor_pago").sum(),
        )
        .sort("ano_emenda")
    )

    silver_favorecidos_totals = (
        favorecidos.group_by("ano_emenda")
        .agg(pl.col("valor_recebido").sum())
        .sort("ano_emenda")
    )

    gold_favorecidos_totals = (
        ranking_favorecidos.group_by("ano_emenda")
        .agg(pl.col("valor_recebido").sum())
        .sort("ano_emenda")
    )

    execucao_comparacao = silver_emendas_totals.join(
        gold_parlamentares_totals,
        on="ano_emenda",
        how="full",
        suffix="_gold",
        coalesce=True,
    )

    favorecidos_comparacao = silver_favorecidos_totals.join(
        gold_favorecidos_totals,
        on="ano_emenda",
        how="full",
        suffix="_gold",
        coalesce=True,
    )

    resultados: list[dict[str, object]] = []

    for row in execucao_comparacao.iter_rows(named=True):
        for metric in (
            "valor_empenhado",
            "valor_liquidado",
            "valor_pago",
        ):
            silver_value = float(row.get(metric) or 0)
            gold_value = float(row.get(f"{metric}_gold") or 0)
            difference = _difference(
                silver_value,
                gold_value,
            )

            resultados.append(
                {
                    "ano_emenda": row["ano_emenda"],
                    "dataset": "ranking_parlamentares",
                    "metrica": metric,
                    "valor_silver": silver_value,
                    "valor_gold": gold_value,
                    "diferenca": difference,
                    "reconciliado": _is_reconciled(difference),
                }
            )

    for row in favorecidos_comparacao.iter_rows(named=True):
        silver_value = float(row.get("valor_recebido") or 0)
        gold_value = float(row.get("valor_recebido_gold") or 0)
        difference = _difference(
            silver_value,
            gold_value,
        )

        resultados.append(
            {
                "ano_emenda": row["ano_emenda"],
                "dataset": "ranking_favorecidos",
                "metrica": "valor_recebido",
                "valor_silver": silver_value,
                "valor_gold": gold_value,
                "diferenca": difference,
                "reconciliado": _is_reconciled(difference),
            }
        )

    resultado_df = pl.DataFrame(resultados)

    output_path = gold_root / "quality" / "reconciliacao_emendas.parquet"
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    resultado_df.write_parquet(
        output_path,
        compression="zstd",
    )

    resultado_df.write_csv(
        output_path.with_suffix(".csv"),
        separator=";",
    )

    all_reconciled = bool(resultado_df["reconciliado"].all())

    manifest = {
        "subject": "emendas",
        "years": years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "checks": resultado_df.height,
        "all_reconciled": all_reconciled,
        "tolerance": TOLERANCE,
        "result_file": str(output_path),
    }

    manifest_path = output_path.parent / "reconciliacao.manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Reconciliação concluída: verificações=%s aprovado=%s",
        resultado_df.height,
        all_reconciled,
    )

    if not all_reconciled:
        failed = resultado_df.filter(~pl.col("reconciliado"))

        raise ValueError(f"A reconciliação encontrou diferenças: {failed.to_dicts()}")

    return manifest_path
