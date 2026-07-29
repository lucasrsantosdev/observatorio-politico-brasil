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


def _as_float(value: object) -> float:
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
        "severity": "info" if approved else "error",
        "approved": approved,
    }


def run_reconciliation_licitacoes(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    period_label = f"{first_period}_{last_period}"

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    gold_root = (
        Path("data/gold")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    licitacoes = _load_dataset(
        silver_root,
        "licitacoes",
    )

    itens = _load_dataset(
        silver_root,
        "itens_licitacao",
    )

    concorrencia = _load_dataset(
        gold_root,
        "concorrencia_licitacoes",
    )

    ranking_fornecedores = _load_dataset(
        gold_root,
        "ranking_fornecedores",
    )

    ranking_modalidades = _load_dataset(
        gold_root,
        "ranking_modalidades",
    )

    ranking_orgaos = _load_dataset(
        gold_root,
        "ranking_orgaos",
    )

    ranking_uf = _load_dataset(
        gold_root,
        "ranking_uf",
    )

    relacionamento = _load_dataset(
        gold_root,
        "relacionamento_orgao_fornecedor",
    )

    total_valor_licitacao = _as_float(licitacoes["valor_licitacao"].sum())

    total_valor_itens = _as_float(itens["valor_item"].sum())

    distinct_fornecedores = (
        itens.select(
            [
                "codigo_vencedor",
                "nome_vencedor",
            ]
        )
        .unique()
        .height
    )

    distinct_modalidades = (
        licitacoes.select(
            [
                "codigo_modalidade_compra",
                "modalidade_compra",
            ]
        )
        .unique()
        .height
    )

    distinct_orgaos = (
        licitacoes.select(
            [
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
            ]
        )
        .unique()
        .height
    )

    distinct_localidades = (
        licitacoes.select(
            [
                "uf",
                "municipio",
            ]
        )
        .unique()
        .height
    )

    distinct_relationships = (
        itens.select(
            [
                "codigo_orgao",
                "nome_orgao",
                "codigo_vencedor",
                "nome_vencedor",
            ]
        )
        .unique()
        .height
    )

    checks = [
        _check_equal(
            check="quantidade_licitacoes",
            silver_value=licitacoes.height,
            gold_value=concorrencia.height,
        ),
        _check_equal(
            check="chaves_licitacao_unicas_silver",
            silver_value=licitacoes.height,
            gold_value=licitacoes["chave_licitacao"].n_unique(),
        ),
        _check_equal(
            check="chaves_licitacao_unicas_gold",
            silver_value=concorrencia.height,
            gold_value=concorrencia["chave_licitacao"].n_unique(),
        ),
        _check_equal(
            check="valor_total_licitacoes",
            silver_value=total_valor_licitacao,
            gold_value=_as_float(concorrencia["valor_licitacao"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="ranking_modalidades_quantidade",
            silver_value=licitacoes.height,
            gold_value=_as_float(ranking_modalidades["quantidade_licitacoes"].sum()),
        ),
        _check_equal(
            check="ranking_modalidades_valor",
            silver_value=total_valor_licitacao,
            gold_value=_as_float(ranking_modalidades["valor_total_licitado"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="ranking_orgaos_quantidade",
            silver_value=licitacoes.height,
            gold_value=_as_float(ranking_orgaos["quantidade_licitacoes"].sum()),
        ),
        _check_equal(
            check="ranking_orgaos_valor",
            silver_value=total_valor_licitacao,
            gold_value=_as_float(ranking_orgaos["valor_total_licitado"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="ranking_uf_quantidade",
            silver_value=licitacoes.height,
            gold_value=_as_float(ranking_uf["quantidade_licitacoes"].sum()),
        ),
        _check_equal(
            check="ranking_uf_valor",
            silver_value=total_valor_licitacao,
            gold_value=_as_float(ranking_uf["valor_total_licitado"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="quantidade_itens_fornecedores",
            silver_value=itens.height,
            gold_value=_as_float(
                ranking_fornecedores["quantidade_itens_vencidos"].sum()
            ),
        ),
        _check_equal(
            check="valor_itens_fornecedores",
            silver_value=total_valor_itens,
            gold_value=_as_float(ranking_fornecedores["valor_itens_vencidos"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="quantidade_itens_relacionamento",
            silver_value=itens.height,
            gold_value=_as_float(relacionamento["quantidade_itens"].sum()),
        ),
        _check_equal(
            check="valor_itens_relacionamento",
            silver_value=total_valor_itens,
            gold_value=_as_float(relacionamento["valor_itens_vencidos"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="valor_itens_concorrencia",
            silver_value=total_valor_itens,
            gold_value=_as_float(concorrencia["valor_itens_vencedores"].sum()),
            tolerance=MONEY_TOLERANCE,
        ),
        _check_equal(
            check="quantidade_fornecedores",
            silver_value=distinct_fornecedores,
            gold_value=ranking_fornecedores.height,
        ),
        _check_equal(
            check="quantidade_modalidades",
            silver_value=distinct_modalidades,
            gold_value=ranking_modalidades.height,
        ),
        _check_equal(
            check="quantidade_orgaos",
            silver_value=distinct_orgaos,
            gold_value=ranking_orgaos.height,
        ),
        _check_equal(
            check="quantidade_localidades",
            silver_value=distinct_localidades,
            gold_value=ranking_uf.height,
        ),
        _check_equal(
            check="quantidade_relacionamentos",
            silver_value=distinct_relationships,
            gold_value=relacionamento.height,
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

    parquet_path = destination / "reconciliacao_licitacoes.parquet"

    csv_path = destination / "reconciliacao_licitacoes.csv"

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
        "subject": "licitacoes",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": reconciliation.height,
        "approved": approved,
        "failed_checks": failed_checks,
        "methodology_notes": [
            (
                "A reconciliação financeira dos itens "
                "utiliza valor_item, conforme a metodologia "
                "da camada Gold."
            ),
            (
                "valor_total_item_calculado não participa "
                "da reconciliação porque quantidade_item e "
                "valor_item não representam necessariamente "
                "quantidade e preço unitário compatíveis."
            ),
        ],
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
        "Reconciliação de licitações concluída: verificações=%s aprovado=%s",
        reconciliation.height,
        approved,
    )

    return manifest_path
