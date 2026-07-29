from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

MONEY_TOLERANCE = 0.05


def _load_dataset(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset não encontrado: {path}")

    return pl.read_parquet(path)


def _as_float(
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
) -> dict[str, object]:
    difference = _as_float(gold_value) - _as_float(silver_value)

    approved = abs(difference) <= tolerance

    return {
        "check": check,
        "silver_value": silver_value,
        "gold_value": gold_value,
        "difference": difference,
        "tolerance": tolerance,
        "severity": ("info" if approved else "error"),
        "approved": approved,
    }


def run_reconciliation_convenios(
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

    gold_root = (
        Path("data/gold") / "portal_transparencia" / "convenios" / f"anos={years_label}"
    )

    silver_path = silver_root / "convenios" / "convenios.parquet"

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver de convênios não encontrada: {silver_path}")

    silver = pl.read_parquet(silver_path)

    fato = _load_dataset(
        gold_root,
        "fato_convenios",
    )

    relacionamento = _load_dataset(
        gold_root,
        "relacionamento_emenda_convenio",
    )

    ranking_convenentes = _load_dataset(
        gold_root,
        "ranking_convenentes",
    )

    ranking_funcoes = _load_dataset(
        gold_root,
        "ranking_funcoes",
    )

    ranking_localidades = _load_dataset(
        gold_root,
        "ranking_localidades",
    )

    resumo_anual = _load_dataset(
        gold_root,
        "resumo_anual",
    )

    valor_silver_grupo_fisico = (
        silver.group_by("numero_convenio")
        .agg(pl.col("valor_convenio").first().alias("valor_convenio"))["valor_convenio"]
        .sum()
    )

    valor_fato = fato["valor_convenio"].sum()

    valor_ranking_convenentes = ranking_convenentes["valor_total_convenios"].sum()

    valor_ranking_funcoes = ranking_funcoes["valor_total_convenios"].sum()

    valor_ranking_localidades = ranking_localidades["valor_total_convenios"].sum()

    valor_resumo_anual = resumo_anual["valor_total_convenios"].sum()

    checks = [
        _check_equal(
            check="registros_relacionamento",
            silver_value=silver.height,
            gold_value=relacionamento.height,
        ),
        _check_equal(
            check="convenios_fisicos",
            silver_value=silver["numero_convenio"].n_unique(),
            gold_value=fato.height,
        ),
        _check_equal(
            check="convenios_fisicos_unicos",
            silver_value=fato.height,
            gold_value=fato["numero_convenio"].n_unique(),
        ),
        _check_equal(
            check="pares_emenda_convenio",
            silver_value=silver.select(
                [
                    "codigo_emenda",
                    "numero_convenio",
                    "ano_emenda",
                    "tipo_emenda",
                ]
            )
            .unique()
            .height,
            gold_value=relacionamento.height,
        ),
        _check_equal(
            check="chaves_relacionamento_unicas",
            silver_value=relacionamento.height,
            gold_value=relacionamento["chave_emenda_convenio"].n_unique(),
        ),
        _check_equal(
            check="valor_fisico_convenios",
            silver_value=_as_float(valor_silver_grupo_fisico),
            gold_value=_as_float(valor_fato),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="valor_ranking_convenentes",
            silver_value=_as_float(valor_fato),
            gold_value=_as_float(valor_ranking_convenentes),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="valor_ranking_funcoes",
            silver_value=_as_float(valor_fato),
            gold_value=_as_float(valor_ranking_funcoes),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="valor_ranking_localidades",
            silver_value=_as_float(valor_fato),
            gold_value=_as_float(valor_ranking_localidades),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="valor_resumo_anual",
            silver_value=_as_float(valor_fato),
            gold_value=_as_float(valor_resumo_anual),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="quantidade_convenentes",
            silver_value=fato.select(
                pl.col("convenente").fill_null("NÃO INFORMADO")
            ).n_unique(),
            gold_value=ranking_convenentes.height,
        ),
        _check_equal(
            check="quantidade_funcoes",
            silver_value=fato.select(
                [
                    "codigo_funcao",
                    "nome_funcao",
                ]
            )
            .unique()
            .height,
            gold_value=ranking_funcoes.height,
        ),
        _check_equal(
            check="quantidade_localidades",
            silver_value=fato.select(
                pl.col("localidade_gasto").fill_null("NÃO INFORMADA")
            ).n_unique(),
            gold_value=ranking_localidades.height,
        ),
        _check_equal(
            check="quantidade_anos_resumo",
            silver_value=fato["primeiro_ano_emenda"].n_unique(),
            gold_value=resumo_anual.height,
        ),
        _check_equal(
            check="total_emendas_relacionadas",
            silver_value=silver["codigo_emenda"].n_unique(),
            gold_value=relacionamento["codigo_emenda"].n_unique(),
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

    parquet_path = destination / "reconciliacao_convenios.parquet"

    csv_path = destination / "reconciliacao_convenios.csv"

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
        "subject": "emendas_convenios",
        "years": sorted(years),
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
        "Reconciliação de convênios concluída: verificações=%s aprovado=%s",
        reconciliation.height,
        approved,
    )

    return manifest_path
