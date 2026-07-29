from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

MONEY_TOLERANCE = 0.05


def _load(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset não encontrado: {path}")

    return pl.read_parquet(path)


def _check(
    *,
    check: str,
    silver_value: float,
    gold_value: float,
    tolerance: float = 0.0,
) -> dict[str, object]:
    difference = float(gold_value) - float(silver_value)
    approved = abs(difference) <= tolerance

    return {
        "check": check,
        "silver_value": silver_value,
        "gold_value": gold_value,
        "difference": difference,
        "tolerance": tolerance,
        "severity": "info" if approved else "error",
        "approved": approved,
    }


def run_reconciliation_gastos_deputados(
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

    gold_root = (
        Path("data/gold")
        / "camara_deputados"
        / "gastos_deputados"
        / f"anos={years_label}"
    )

    silver = _load(
        silver_root,
        "despesas",
    )

    fato = _load(
        gold_root,
        "fato_gastos_deputados",
    )

    ranking_deputados = _load(
        gold_root,
        "ranking_deputados",
    )

    ranking_partidos = _load(
        gold_root,
        "ranking_partidos",
    )

    ranking_ufs = _load(
        gold_root,
        "ranking_ufs",
    )

    ranking_fornecedores = _load(
        gold_root,
        "ranking_fornecedores",
    )

    ranking_tipos = _load(
        gold_root,
        "ranking_tipos_despesa",
    )

    resumo_mensal = _load(
        gold_root,
        "resumo_mensal",
    )

    parlamentares = fato.filter(pl.col("tipo_beneficiario") == "PARLAMENTAR")

    total_documento = float(silver["valor_documento"].sum() or 0.0)

    total_glosa = float(silver["valor_glosa"].sum() or 0.0)

    total_liquido = float(silver["valor_liquido"].sum() or 0.0)

    checks = [
        _check(
            check="registros_fato",
            silver_value=silver.height,
            gold_value=fato.height,
        ),
        _check(
            check="chaves_despesa_unicas",
            silver_value=silver.height,
            gold_value=fato["chave_despesa"].n_unique(),
        ),
        _check(
            check="valor_documento_fato",
            silver_value=total_documento,
            gold_value=float(fato["valor_documento"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="valor_glosa_fato",
            silver_value=total_glosa,
            gold_value=float(fato["valor_glosa"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="valor_liquido_fato",
            silver_value=total_liquido,
            gold_value=float(fato["valor_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_deputados",
            silver_value=parlamentares["chave_beneficiario"].n_unique(),
            gold_value=ranking_deputados.height,
        ),
        _check(
            check="valor_ranking_deputados",
            silver_value=float(parlamentares["valor_liquido"].sum() or 0.0),
            gold_value=float(ranking_deputados["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_partidos",
            silver_value=parlamentares.select(
                pl.col("partido").fill_null("NÃO INFORMADO")
            ).n_unique(),
            gold_value=ranking_partidos.height,
        ),
        _check(
            check="valor_ranking_partidos",
            silver_value=float(parlamentares["valor_liquido"].sum() or 0.0),
            gold_value=float(ranking_partidos["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_ufs",
            silver_value=parlamentares.select(
                pl.col("uf").fill_null("NÃO INFORMADA")
            ).n_unique(),
            gold_value=ranking_ufs.height,
        ),
        _check(
            check="valor_ranking_ufs",
            silver_value=float(parlamentares["valor_liquido"].sum() or 0.0),
            gold_value=float(ranking_ufs["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_fornecedores",
            silver_value=fato["chave_fornecedor"].n_unique(),
            gold_value=ranking_fornecedores.height,
        ),
        _check(
            check="valor_ranking_fornecedores",
            silver_value=total_liquido,
            gold_value=float(ranking_fornecedores["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_tipos_despesa",
            silver_value=fato.select(
                [
                    "codigo_tipo_despesa",
                    "tipo_despesa",
                ]
            )
            .unique()
            .height,
            gold_value=ranking_tipos.height,
        ),
        _check(
            check="valor_ranking_tipos_despesa",
            silver_value=total_liquido,
            gold_value=float(ranking_tipos["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_periodos",
            silver_value=fato["periodo_despesa"].n_unique(),
            gold_value=resumo_mensal.height,
        ),
        _check(
            check="valor_resumo_mensal",
            silver_value=total_liquido,
            gold_value=float(resumo_mensal["valor_total_liquido"].sum() or 0.0),
            tolerance=MONEY_TOLERANCE,
        ),
        _check(
            check="quantidade_estornos",
            silver_value=silver.filter(pl.col("valor_liquido") < 0).height,
            gold_value=fato.filter(pl.col("movimento_negativo")).height,
        ),
    ]

    reconciliation = pl.DataFrame(
        checks,
        infer_schema_length=None,
    )

    approved = bool(reconciliation["approved"].all())

    destination = gold_root / "reconciliation"

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / "reconciliacao_gastos_deputados.parquet"

    csv_path = destination / "reconciliacao_gastos_deputados.csv"

    reconciliation.write_parquet(
        parquet_path,
        compression="zstd",
    )

    reconciliation.write_csv(
        csv_path,
        separator=";",
    )

    manifest = {
        "source": "camara_deputados",
        "subject": "gastos_deputados_ceap",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": reconciliation.height,
        "approved": approved,
        "failed_checks": reconciliation.filter(~pl.col("approved")).to_dicts(),
        "reconciliation_parquet": str(parquet_path),
        "reconciliation_csv": str(csv_path),
    }

    manifest_path = destination / "reconciliation.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Reconciliação de gastos concluída: verificações=%s aprovado=%s",
        reconciliation.height,
        approved,
    )

    return manifest_path
