from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


SILVER_ROOT = Path("data/silver/senado_federal/anos=2025_2026")

GOLD_ROOT = Path("data/gold/senado_federal/anos=2025_2026")


def _load(
    dataset: str,
) -> pl.DataFrame:
    path = SILVER_ROOT / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Silver nao encontrada: {path}")

    return pl.read_parquet(path)


def _write(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
) -> dict[str, Any]:
    destination = GOLD_ROOT / dataset

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dataset}.parquet"

    csv_path = destination / f"{dataset}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    logger.info(
        "Gold Senado criada: dataset=%s registros=%s colunas=%s",
        dataset,
        dataframe.height,
        dataframe.width,
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def _first_existing(
    dataframe: pl.DataFrame,
    names: tuple[str, ...],
) -> str | None:
    for name in names:
        if name in dataframe.columns:
            return name

    return None


def _build_fato_materias(
    materias: pl.DataFrame,
) -> pl.DataFrame:
    return materias.with_columns(
        pl.col("codigo").cast(pl.Int64).alias("id_materia"),
        pl.concat_str(
            [
                pl.col("sigla").fill_null(""),
                pl.col("numero").cast(pl.String).fill_null(""),
                pl.col("ano").cast(pl.String).fill_null(""),
            ],
            separator=" ",
        ).alias("titulo_materia"),
        pl.col("data").dt.year().alias("ano_apresentacao"),
        pl.col("data").dt.month().cast(pl.Int8).alias("mes_apresentacao"),
        pl.col("data").dt.strftime("%Y%m").alias("periodo_apresentacao"),
        pl.col("ementa").is_not_null().alias("possui_ementa"),
    )


def _build_fato_votacoes(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return votacoes.with_columns(
        pl.col("codigo_sessao_votacao").cast(pl.Int64).alias("id_votacao"),
        pl.col("data_sessao").dt.year().alias("ano_votacao"),
        pl.col("data_sessao").dt.month().cast(pl.Int8).alias("mes_votacao"),
        pl.col("data_sessao").dt.strftime("%Y%m").alias("periodo_votacao"),
        (
            pl.col("total_votos_sim").fill_null(0)
            + pl.col("total_votos_nao").fill_null(0)
            + pl.col("total_votos_abstencao").fill_null(0)
        ).alias("total_votos_informados"),
        pl.when(pl.col("resultado_votacao").is_in(["A", "APROVADO", "APROVADA"]))
        .then(pl.lit("APROVADA"))
        .when(pl.col("resultado_votacao").is_in(["R", "REJEITADO", "REJEITADA"]))
        .then(pl.lit("REJEITADA"))
        .otherwise(
            pl.col("resultado_votacao").fill_null("SEM_RESULTADO").str.to_uppercase()
        )
        .alias("resultado_votacao_normalizado"),
    )


def _build_fato_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    if (
        "sigla_uf_parlamentar" not in votos.columns
        and "sigla_ufparlamentar" in votos.columns
    ):
        votos = votos.rename({"sigla_ufparlamentar": ("sigla_uf_parlamentar")})

    return votos.with_columns(
        pl.concat_str(
            [
                pl.col("codigo_sessao_votacao").cast(pl.String),
                pl.col("codigo_parlamentar").cast(pl.String),
                pl.col("linha_origem").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_voto"),
        pl.col("data_sessao").dt.year().alias("ano_voto"),
        pl.col("data_sessao").dt.month().cast(pl.Int8).alias("mes_voto"),
        pl.col("data_sessao").dt.strftime("%Y%m").alias("periodo_voto"),
        pl.when(pl.col("sigla_voto_parlamentar").str.to_uppercase().is_in(["SIM", "S"]))
        .then(pl.lit("SIM"))
        .when(
            pl.col("sigla_voto_parlamentar")
            .str.to_uppercase()
            .is_in(["NAO", "NÃO", "N"])
        )
        .then(pl.lit("NAO"))
        .when(pl.col("sigla_voto_parlamentar").str.to_uppercase().str.contains("ABST"))
        .then(pl.lit("ABSTENCAO"))
        .when(pl.col("sigla_voto_parlamentar").str.to_uppercase().str.contains("VOTOU"))
        .then(pl.lit("VOTOU"))
        .otherwise(
            pl.col("sigla_voto_parlamentar")
            .fill_null("NAO_INFORMADO")
            .str.to_uppercase()
        )
        .alias("categoria_voto"),
    )


def _build_fato_gastos(
    ceaps: pl.DataFrame,
) -> pl.DataFrame:
    return ceaps.with_columns(
        pl.col("id").cast(pl.Int64).alias("id_gasto"),
        pl.col("data").dt.year().alias("ano_gasto"),
        pl.col("data").dt.month().cast(pl.Int8).alias("mes_gasto"),
        pl.col("data").dt.strftime("%Y%m").alias("periodo_gasto"),
        pl.col("cpf_cnpj_fornecedor")
        .str.replace_all(
            r"[^0-9]",
            "",
        )
        .alias("cpf_cnpj_fornecedor_normalizado"),
        pl.col("valor_reembolsado").fill_null(0.0).alias("valor_gasto"),
    )


def _build_ranking_senadores_gastos(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.group_by(
            [
                "cod_senador",
                "nome_senador",
            ]
        )
        .agg(
            pl.len().alias("quantidade_despesas"),
            pl.col("valor_gasto").sum().round(2).alias("valor_total"),
            pl.col("cpf_cnpj_fornecedor_normalizado")
            .n_unique()
            .alias("quantidade_fornecedores"),
        )
        .with_columns(
            pl.col("valor_total")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "valor_total",
            descending=True,
        )
    )


def _build_ranking_fornecedores(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.group_by(
            [
                "cpf_cnpj_fornecedor_normalizado",
                "nome_fornecedor",
            ]
        )
        .agg(
            pl.len().alias("quantidade_despesas"),
            pl.col("valor_gasto").sum().round(2).alias("valor_total"),
            pl.col("cod_senador").n_unique().alias("quantidade_senadores"),
        )
        .with_columns(
            pl.col("valor_total")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "valor_total",
            descending=True,
        )
    )


def _build_ranking_tipos_despesa(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.group_by("tipo_despesa")
        .agg(
            pl.len().alias("quantidade_despesas"),
            pl.col("valor_gasto").sum().round(2).alias("valor_total"),
            pl.col("cod_senador").n_unique().alias("quantidade_senadores"),
        )
        .with_columns(
            pl.col("valor_total")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "valor_total",
            descending=True,
        )
    )


def _build_ranking_senadores_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.group_by(
            [
                "codigo_parlamentar",
                "nome_parlamentar",
                "sigla_partido_parlamentar",
                "sigla_uf_parlamentar",
            ]
        )
        .agg(
            pl.len().alias("quantidade_votos"),
            pl.col("codigo_sessao_votacao").n_unique().alias("quantidade_votacoes"),
            (pl.col("categoria_voto") == "SIM").sum().alias("votos_sim"),
            (pl.col("categoria_voto") == "NAO").sum().alias("votos_nao"),
            (pl.col("categoria_voto") == "ABSTENCAO").sum().alias("abstencoes"),
        )
        .with_columns(
            pl.col("quantidade_votos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "quantidade_votos",
            descending=True,
        )
    )


def _build_ranking_partidos_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.with_columns(
            pl.col("sigla_partido_parlamentar")
            .fill_null("SEM_PARTIDO")
            .alias("sigla_partido")
        )
        .group_by("sigla_partido")
        .agg(
            pl.col("codigo_parlamentar").n_unique().alias("quantidade_senadores"),
            pl.len().alias("quantidade_votos"),
            (pl.col("categoria_voto") == "SIM").sum().alias("votos_sim"),
            (pl.col("categoria_voto") == "NAO").sum().alias("votos_nao"),
            (pl.col("categoria_voto") == "ABSTENCAO").sum().alias("abstencoes"),
        )
        .with_columns(
            pl.col("quantidade_votos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "quantidade_votos",
            descending=True,
        )
    )


def _build_resumo_gastos_mensal(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.group_by(
            [
                "ano_gasto",
                "mes_gasto",
                "periodo_gasto",
            ]
        )
        .agg(
            pl.len().alias("quantidade_despesas"),
            pl.col("valor_gasto").sum().round(2).alias("valor_total"),
            pl.col("cod_senador").n_unique().alias("quantidade_senadores"),
            pl.col("cpf_cnpj_fornecedor_normalizado")
            .n_unique()
            .alias("quantidade_fornecedores"),
        )
        .sort(
            [
                "ano_gasto",
                "mes_gasto",
            ]
        )
    )


def _build_resumo_atividade_mensal(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votacoes.group_by(
            [
                "ano_votacao",
                "mes_votacao",
                "periodo_votacao",
            ]
        )
        .agg(
            pl.len().alias("quantidade_votacoes"),
            (pl.col("resultado_votacao_normalizado") == "APROVADA")
            .sum()
            .alias("quantidade_aprovadas"),
            (pl.col("resultado_votacao_normalizado") == "REJEITADA")
            .sum()
            .alias("quantidade_rejeitadas"),
            pl.col("total_votos_informados").sum().alias("total_votos_informados"),
        )
        .sort(
            [
                "ano_votacao",
                "mes_votacao",
            ]
        )
    )


def run_senado_gold() -> Path:
    manifest_path = SILVER_ROOT / "silver.manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if not manifest.get(
        "approved",
        False,
    ):
        raise RuntimeError("Silver do Senado nao aprovada.")

    materias = _load("materias")
    votacoes = _load("votacoes")
    votos = _load("votos")
    ceaps = _load("ceaps")
    senadores = _load("senadores")
    empresas = _load("empresas_contratadas")

    fato_materias = _build_fato_materias(materias)

    fato_votacoes = _build_fato_votacoes(votacoes)

    fato_votos = _build_fato_votos(votos)

    fato_gastos = _build_fato_gastos(ceaps)

    datasets = {
        "fato_materias": fato_materias,
        "fato_votacoes": fato_votacoes,
        "fato_votos": fato_votos,
        "fato_gastos_senadores": (fato_gastos),
        "dim_senadores_base": (senadores),
        "dim_empresas_contratadas_base": (empresas),
        "ranking_senadores_gastos": (_build_ranking_senadores_gastos(fato_gastos)),
        "ranking_fornecedores_ceaps": (_build_ranking_fornecedores(fato_gastos)),
        "ranking_tipos_despesa_ceaps": (_build_ranking_tipos_despesa(fato_gastos)),
        "ranking_senadores_votos": (_build_ranking_senadores_votos(fato_votos)),
        "ranking_partidos_votos": (_build_ranking_partidos_votos(fato_votos)),
        "resumo_gastos_mensal": (_build_resumo_gastos_mensal(fato_gastos)),
        "resumo_atividade_mensal": (_build_resumo_atividade_mensal(fato_votacoes)),
    }

    outputs = [
        _write(
            dataframe,
            dataset=dataset,
        )
        for dataset, dataframe in datasets.items()
    ]

    gold_manifest = {
        "source": "senado_federal",
        "layer": "gold",
        "years": [2025, 2026],
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "silver_approved": True,
        "dataset_count": len(outputs),
        "approved": all(int(item["record_count"]) > 0 for item in outputs),
        "datasets": outputs,
    }

    output_manifest = GOLD_ROOT / "gold.manifest.json"

    output_manifest.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_manifest.write_text(
        json.dumps(
            gold_manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("GOLD DO SENADO FEDERAL")
    print("=" * 100)

    for item in outputs:
        print(
            f"{item['dataset']:<40} "
            f"registros="
            f"{item['record_count']:<10} "
            f"colunas="
            f"{item['column_count']}"
        )

    print()
    print(f"DATASET_COUNT={len(outputs)}")
    print(f"GOLD_APPROVED={gold_manifest['approved']}")

    return output_manifest
