from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

TOLERANCE = 0.05


def _load_parquet(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset não encontrado: {path}")

    return pl.read_parquet(path)


def _number(
    value: object,
) -> float:
    if value is None:
        return 0.0

    return float(value)


def _check_equal(
    *,
    check: str,
    silver_value: float,
    gold_value: float,
    tolerance: float = 0.0,
    severity: str = "error",
) -> dict[str, object]:
    difference = _number(gold_value) - _number(silver_value)

    approved = abs(difference) <= tolerance

    return {
        "check": check,
        "silver_value": silver_value,
        "gold_value": gold_value,
        "difference": difference,
        "tolerance": tolerance,
        "severity": ("info" if approved else severity),
        "approved": (approved or severity == "warning"),
    }


def run_reconciliation_contratos(
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

    contratos_silver = _load_parquet(
        silver_root,
        "contratos_atuais",
    )

    itens_silver = _load_parquet(
        silver_root,
        "itens_contrato",
    )

    termos_silver = _load_parquet(
        silver_root,
        "termos_aditivos",
    )

    detalhe_gold = _load_parquet(
        gold_root,
        "contratos_atuais_detalhe",
    )

    itens_gold = _load_parquet(
        gold_root,
        "itens_contrato_atuais",
    )

    resumo_gold = _load_parquet(
        gold_root,
        "contratos_atuais_resumo",
    )

    ranking_contratados = _load_parquet(
        gold_root,
        "ranking_contratados",
    )

    ranking_orgaos = _load_parquet(
        gold_root,
        "ranking_orgaos",
    )

    termos_gold = _load_parquet(
        gold_root,
        "termos_por_contrato",
    )

    relacionamentos = _load_parquet(
        gold_root,
        "relacionamento_orgao_contratado",
    )

    itens_silver_preparados = itens_silver.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("codigo_item_compra").fill_null(""),
            ],
            separator="|",
        ).alias("chave_logica_item")
    )

    itens_identificados = itens_silver_preparados.filter(~pl.col("item_codigo_zero"))

    periodos_mais_recentes = itens_identificados.group_by("chave_logica_item").agg(
        pl.col("periodo_origem").max().alias("periodo_mais_recente")
    )

    itens_identificados_atuais = itens_identificados.join(
        periodos_mais_recentes,
        on="chave_logica_item",
        how="inner",
    ).filter(pl.col("periodo_origem") == pl.col("periodo_mais_recente"))

    itens_codigo_zero = itens_silver_preparados.filter(pl.col("item_codigo_zero"))

    silver_valor_inicial = contratos_silver["valor_inicial_compra"].sum()

    silver_valor_final = contratos_silver["valor_final_compra"].sum()

    silver_variacao = contratos_silver["variacao_valor_contrato"].sum()

    gold_valor_inicial = resumo_gold["valor_inicial_total_grupo"].sum()

    gold_valor_final = resumo_gold["valor_final_total_grupo"].sum()

    gold_variacao = resumo_gold["variacao_total_grupo"].sum()

    silver_valor_itens_identificados = itens_identificados_atuais[
        "valor_total_item_calculado"
    ].sum()

    silver_valor_itens_codigo_zero = itens_codigo_zero[
        "valor_total_item_calculado"
    ].sum()

    gold_valor_itens_identificados = resumo_gold[
        "valor_total_itens_identificados"
    ].sum()

    gold_valor_itens_codigo_zero = resumo_gold["valor_total_itens_codigo_zero"].sum()

    checks: list[dict[str, object]] = [
        _check_equal(
            check="contratos_fisicos",
            silver_value=contratos_silver.height,
            gold_value=detalhe_gold.height,
        ),
        _check_equal(
            check="grupos_contrato",
            silver_value=contratos_silver["chave_grupo_contrato"].n_unique(),
            gold_value=resumo_gold.height,
        ),
        _check_equal(
            check="grupos_unicos_resumo",
            silver_value=resumo_gold.height,
            gold_value=resumo_gold["chave_grupo_contrato"].n_unique(),
        ),
        _check_equal(
            check="valor_inicial_contratos",
            silver_value=_number(silver_valor_inicial),
            gold_value=_number(gold_valor_inicial),
            tolerance=TOLERANCE,
        ),
        _check_equal(
            check="valor_final_contratos",
            silver_value=_number(silver_valor_final),
            gold_value=_number(gold_valor_final),
            tolerance=TOLERANCE,
        ),
        _check_equal(
            check="variacao_valor_contratos",
            silver_value=_number(silver_variacao),
            gold_value=_number(gold_variacao),
            tolerance=TOLERANCE,
        ),
        _check_equal(
            check="itens_identificados_atuais",
            silver_value=(itens_identificados_atuais.height),
            gold_value=itens_gold.filter(
                pl.col("item_utilizavel_valor_oficial")
            ).height,
        ),
        _check_equal(
            check="itens_codigo_zero",
            silver_value=itens_codigo_zero.height,
            gold_value=itens_gold.filter(pl.col("item_codigo_zero")).height,
        ),
        _check_equal(
            check="valor_itens_identificados",
            silver_value=_number(silver_valor_itens_identificados),
            gold_value=_number(gold_valor_itens_identificados),
            tolerance=TOLERANCE,
        ),
        _check_equal(
            check="valor_itens_codigo_zero",
            silver_value=_number(silver_valor_itens_codigo_zero),
            gold_value=_number(gold_valor_itens_codigo_zero),
            tolerance=TOLERANCE,
        ),
        _check_equal(
            check="termos_aditivos",
            silver_value=termos_silver["chave_termo_aditivo"].n_unique(),
            gold_value=termos_gold["quantidade_termos"].sum(),
        ),
        _check_equal(
            check="grupos_termos",
            silver_value=contratos_silver["chave_grupo_contrato"].n_unique(),
            gold_value=termos_gold.height,
        ),
        _check_equal(
            check="contratados_ranking",
            silver_value=contratos_silver.filter(
                pl.col("codigo_contratado").is_not_null()
            )["codigo_contratado"].n_unique(),
            gold_value=ranking_contratados.height,
        ),
        _check_equal(
            check="orgaos_ranking",
            silver_value=contratos_silver.select(
                [
                    "codigo_orgao_superior",
                    "codigo_orgao",
                ]
            )
            .unique()
            .height,
            gold_value=ranking_orgaos.height,
        ),
        _check_equal(
            check="relacionamentos_orgao_contratado",
            silver_value=contratos_silver.filter(
                pl.col("codigo_contratado").is_not_null()
            )
            .select(
                [
                    "codigo_orgao_superior",
                    "nome_orgao_superior",
                    "codigo_orgao",
                    "nome_orgao",
                    "codigo_contratado",
                    "nome_contratado",
                ]
            )
            .unique()
            .height,
            gold_value=relacionamentos.height,
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

    parquet_path = destination / "reconciliacao_contratos.parquet"

    csv_path = destination / "reconciliacao_contratos.csv"

    reconciliation.write_parquet(
        parquet_path,
        compression="zstd",
    )

    reconciliation.write_csv(
        csv_path,
        separator=";",
    )

    failed_checks = reconciliation.filter(~pl.col("approved")).to_dicts()

    manifest = {
        "source": "portal_transparencia",
        "subject": "contratos_federais",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": reconciliation.height,
        "approved": approved,
        "failed_checks": failed_checks,
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
        "Reconciliação de contratos concluída: verificações=%s aprovado=%s",
        reconciliation.height,
        approved,
    )

    return manifest_path
