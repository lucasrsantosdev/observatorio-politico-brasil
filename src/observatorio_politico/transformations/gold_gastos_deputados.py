from __future__ import annotations

import polars as pl


def build_fato_gastos_deputados(
    despesas: pl.DataFrame,
) -> pl.DataFrame:
    return despesas.with_columns(
        (pl.col("valor_liquido") - pl.col("valor_restituicao").fill_null(0.0))
        .round(2)
        .alias("valor_liquido_apos_restituicao"),
        (pl.col("valor_liquido") > 0).alias("movimento_positivo"),
        (pl.col("valor_liquido") < 0).alias("movimento_negativo"),
    ).sort(
        [
            "ano_despesa",
            "mes_despesa",
            "codigo_beneficiario",
            "chave_despesa",
        ]
    )


def build_ranking_deputados(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    parlamentares = fato.filter(pl.col("tipo_beneficiario") == "PARLAMENTAR")

    return (
        parlamentares.group_by(
            [
                "chave_beneficiario",
                "codigo_beneficiario",
                "nome_beneficiario",
                "partido",
                "uf",
            ]
        )
        .agg(
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_documento").n_unique().alias("quantidade_documentos"),
            pl.col("chave_fornecedor").n_unique().alias("quantidade_fornecedores"),
            pl.col("codigo_tipo_despesa").n_unique().alias("quantidade_tipos_despesa"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
            pl.col("valor_restituicao")
            .fill_null(0.0)
            .sum()
            .alias("valor_total_restituicoes"),
            pl.col("valor_liquido_apos_restituicao")
            .sum()
            .alias("valor_liquido_apos_restituicao"),
            (pl.col("valor_liquido") < 0).sum().alias("quantidade_estornos"),
            (~pl.col("financeiro_consistente"))
            .sum()
            .alias("quantidade_inconsistencias_financeiras"),
        )
        .with_columns(
            pl.col("valor_total_liquido")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_liquido"),
            pl.col("quantidade_lancamentos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_quantidade_lancamentos"),
        )
        .sort(
            "valor_total_liquido",
            descending=True,
        )
    )


def build_ranking_partidos(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    parlamentares = fato.filter(pl.col("tipo_beneficiario") == "PARLAMENTAR")

    return (
        parlamentares.with_columns(pl.col("partido").fill_null("NÃO INFORMADO"))
        .group_by("partido")
        .agg(
            pl.col("chave_beneficiario").n_unique().alias("quantidade_parlamentares"),
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_documento").n_unique().alias("quantidade_documentos"),
            pl.col("chave_fornecedor").n_unique().alias("quantidade_fornecedores"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
            pl.col("valor_restituicao")
            .fill_null(0.0)
            .sum()
            .alias("valor_total_restituicoes"),
        )
        .with_columns(
            (pl.col("valor_total_liquido") / pl.col("quantidade_parlamentares"))
            .round(2)
            .alias("valor_medio_por_parlamentar"),
            pl.col("valor_total_liquido")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_liquido"),
        )
        .sort(
            "valor_total_liquido",
            descending=True,
        )
    )


def build_ranking_ufs(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    parlamentares = fato.filter(pl.col("tipo_beneficiario") == "PARLAMENTAR")

    return (
        parlamentares.with_columns(pl.col("uf").fill_null("NÃO INFORMADA"))
        .group_by("uf")
        .agg(
            pl.col("chave_beneficiario").n_unique().alias("quantidade_parlamentares"),
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_fornecedor").n_unique().alias("quantidade_fornecedores"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
        )
        .with_columns(
            (pl.col("valor_total_liquido") / pl.col("quantidade_parlamentares"))
            .round(2)
            .alias("valor_medio_por_parlamentar"),
            pl.col("valor_total_liquido")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_liquido"),
        )
        .sort(
            "valor_total_liquido",
            descending=True,
        )
    )


def build_ranking_fornecedores(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by(
            [
                "chave_fornecedor",
                "documento_fornecedor",
                "nome_fornecedor",
            ]
        )
        .agg(
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_documento").n_unique().alias("quantidade_documentos"),
            pl.col("chave_beneficiario").n_unique().alias("quantidade_beneficiarios"),
            pl.col("partido").drop_nulls().n_unique().alias("quantidade_partidos"),
            pl.col("uf").drop_nulls().n_unique().alias("quantidade_ufs"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
            (pl.col("valor_liquido") < 0).sum().alias("quantidade_estornos"),
        )
        .with_columns(
            pl.col("valor_total_liquido")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_liquido")
        )
        .sort(
            "valor_total_liquido",
            descending=True,
        )
    )


def build_ranking_tipos_despesa(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by(
            [
                "codigo_tipo_despesa",
                "tipo_despesa",
            ]
        )
        .agg(
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_beneficiario").n_unique().alias("quantidade_beneficiarios"),
            pl.col("chave_fornecedor").n_unique().alias("quantidade_fornecedores"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
            pl.col("valor_liquido").mean().round(2).alias("valor_medio_lancamento"),
            (pl.col("valor_liquido") < 0).sum().alias("quantidade_estornos"),
        )
        .with_columns(
            pl.col("valor_total_liquido")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_liquido")
        )
        .sort(
            "valor_total_liquido",
            descending=True,
        )
    )


def build_resumo_mensal(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by(
            [
                "ano_despesa",
                "mes_despesa",
                "periodo_despesa",
            ]
        )
        .agg(
            pl.len().alias("quantidade_lancamentos"),
            pl.col("chave_documento").n_unique().alias("quantidade_documentos"),
            pl.col("chave_beneficiario").n_unique().alias("quantidade_beneficiarios"),
            pl.col("chave_fornecedor").n_unique().alias("quantidade_fornecedores"),
            pl.col("valor_documento").sum().alias("valor_total_documentos"),
            pl.col("valor_glosa").sum().alias("valor_total_glosas"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
            pl.col("valor_restituicao")
            .fill_null(0.0)
            .sum()
            .alias("valor_total_restituicoes"),
            (pl.col("valor_liquido") < 0).sum().alias("quantidade_estornos"),
        )
        .sort(
            [
                "ano_despesa",
                "mes_despesa",
            ]
        )
    )
