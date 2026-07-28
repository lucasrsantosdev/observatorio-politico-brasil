from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)


def build_ranking_parlamentares(
    emendas: pl.DataFrame,
    favorecidos: pl.DataFrame,
) -> pl.DataFrame:
    execucao = emendas.group_by(
        [
            "ano_emenda",
            "codigo_autor_emenda",
            "nome_autor_emenda",
        ]
    ).agg(
        pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
        pl.col("codigo_municipio_ibge").n_unique().alias("quantidade_municipios"),
        pl.col("uf").drop_nulls().n_unique().alias("quantidade_ufs"),
        pl.col("nome_funcao").drop_nulls().n_unique().alias("quantidade_funcoes"),
        pl.col("valor_empenhado").sum().alias("valor_empenhado"),
        pl.col("valor_liquidado").sum().alias("valor_liquidado"),
        pl.col("valor_pago").sum().alias("valor_pago"),
        pl.col("valor_restos_pagar_inscritos")
        .sum()
        .alias("valor_restos_pagar_inscritos"),
        pl.col("valor_restos_pagar_cancelados")
        .sum()
        .alias("valor_restos_pagar_cancelados"),
        pl.col("valor_restos_pagar_pagos").sum().alias("valor_restos_pagar_pagos"),
    )

    recebimentos = favorecidos.group_by(
        [
            "ano_emenda",
            "codigo_autor_emenda",
            "nome_autor_emenda",
        ]
    ).agg(
        pl.col("codigo_favorecido")
        .drop_nulls()
        .n_unique()
        .alias("quantidade_favorecidos"),
        pl.col("valor_recebido").sum().alias("valor_recebido_favorecidos"),
    )

    ranking = execucao.join(
        recebimentos,
        on=[
            "ano_emenda",
            "codigo_autor_emenda",
            "nome_autor_emenda",
        ],
        how="left",
    )

    ranking = ranking.with_columns(
        pl.col("quantidade_favorecidos").fill_null(0),
        pl.col("valor_recebido_favorecidos").fill_null(0.0),
        pl.when(pl.col("valor_empenhado") > 0)
        .then(pl.col("valor_liquidado") / pl.col("valor_empenhado") * 100)
        .otherwise(None)
        .round(4)
        .alias("percentual_liquidado"),
        pl.when(pl.col("valor_empenhado") > 0)
        .then(pl.col("valor_pago") / pl.col("valor_empenhado") * 100)
        .otherwise(None)
        .round(4)
        .alias("percentual_pago"),
        pl.when(pl.col("quantidade_emendas") > 0)
        .then(pl.col("valor_empenhado") / pl.col("quantidade_emendas"))
        .otherwise(None)
        .round(2)
        .alias("valor_medio_por_emenda"),
    )

    ranking = ranking.with_columns(
        pl.col("valor_empenhado")
        .rank(
            method="dense",
            descending=True,
        )
        .over("ano_emenda")
        .cast(pl.Int32)
        .alias("posicao_valor_empenhado"),
        pl.col("valor_pago")
        .rank(
            method="dense",
            descending=True,
        )
        .over("ano_emenda")
        .cast(pl.Int32)
        .alias("posicao_valor_pago"),
        pl.col("percentual_pago")
        .rank(
            method="dense",
            descending=True,
        )
        .over("ano_emenda")
        .cast(pl.Int32)
        .alias("posicao_percentual_pago"),
    )

    return ranking.sort(
        [
            "ano_emenda",
            "valor_pago",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_ranking_favorecidos(
    favorecidos: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        favorecidos.group_by(
            [
                "ano_emenda",
                "codigo_favorecido",
                "favorecido",
                "natureza_juridica",
                "tipo_favorecido",
                "uf_favorecido",
                "municipio_favorecido",
            ]
        )
        .agg(
            pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
            pl.col("codigo_autor_emenda")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_autores"),
            pl.col("ano_mes").n_unique().alias("quantidade_meses_recebimento"),
            pl.col("valor_recebido").sum().alias("valor_recebido"),
        )
        .with_columns(
            pl.col("valor_recebido")
            .rank(
                method="dense",
                descending=True,
            )
            .over("ano_emenda")
            .cast(pl.Int32)
            .alias("posicao_valor_recebido")
        )
    )

    return ranking.sort(
        [
            "ano_emenda",
            "valor_recebido",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_ranking_uf(
    emendas: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        emendas.filter(pl.col("uf").is_not_null())
        .with_columns(pl.col("uf").cast(pl.String).str.strip_chars().str.to_uppercase())
        .group_by(
            [
                "ano_emenda",
                "uf",
                "regiao",
            ]
        )
        .agg(
            pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
            pl.col("codigo_autor_emenda")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_autores"),
            pl.col("codigo_municipio_ibge")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_municipios"),
            pl.col("valor_empenhado").sum().alias("valor_empenhado"),
            pl.col("valor_liquidado").sum().alias("valor_liquidado"),
            pl.col("valor_pago").sum().alias("valor_pago"),
        )
        .with_columns(
            pl.when(pl.col("valor_empenhado") > 0)
            .then(pl.col("valor_pago") / pl.col("valor_empenhado") * 100)
            .otherwise(None)
            .round(4)
            .alias("percentual_pago")
        )
        .with_columns(
            pl.col("valor_pago")
            .rank(
                method="dense",
                descending=True,
            )
            .over("ano_emenda")
            .cast(pl.Int32)
            .alias("posicao_valor_pago")
        )
    )

    return ranking.sort(
        [
            "ano_emenda",
            "valor_pago",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_ranking_municipios(
    emendas: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        emendas.filter(pl.col("codigo_municipio_ibge").is_not_null())
        .with_columns(pl.col("uf").cast(pl.String).str.strip_chars().str.to_uppercase())
        .group_by(
            [
                "ano_emenda",
                "codigo_municipio_ibge",
                "municipio",
                "uf",
                "regiao",
            ]
        )
        .agg(
            pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
            pl.col("codigo_autor_emenda")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_autores"),
            pl.col("nome_funcao").drop_nulls().n_unique().alias("quantidade_funcoes"),
            pl.col("valor_empenhado").sum().alias("valor_empenhado"),
            pl.col("valor_liquidado").sum().alias("valor_liquidado"),
            pl.col("valor_pago").sum().alias("valor_pago"),
        )
        .with_columns(
            pl.when(pl.col("valor_empenhado") > 0)
            .then(pl.col("valor_pago") / pl.col("valor_empenhado") * 100)
            .otherwise(None)
            .round(4)
            .alias("percentual_pago")
        )
        .with_columns(
            pl.col("valor_pago")
            .rank(
                method="dense",
                descending=True,
            )
            .over("ano_emenda")
            .cast(pl.Int32)
            .alias("posicao_valor_pago")
        )
    )

    return ranking.sort(
        [
            "ano_emenda",
            "valor_pago",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_ranking_funcoes(
    emendas: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        emendas.group_by(
            [
                "ano_emenda",
                "codigo_funcao",
                "nome_funcao",
            ]
        )
        .agg(
            pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
            pl.col("codigo_autor_emenda")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_autores"),
            pl.col("codigo_municipio_ibge")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_municipios"),
            pl.col("valor_empenhado").sum().alias("valor_empenhado"),
            pl.col("valor_liquidado").sum().alias("valor_liquidado"),
            pl.col("valor_pago").sum().alias("valor_pago"),
        )
        .with_columns(
            pl.when(pl.col("valor_empenhado") > 0)
            .then(pl.col("valor_pago") / pl.col("valor_empenhado") * 100)
            .otherwise(None)
            .round(4)
            .alias("percentual_pago")
        )
        .with_columns(
            pl.col("valor_pago")
            .rank(
                method="dense",
                descending=True,
            )
            .over("ano_emenda")
            .cast(pl.Int32)
            .alias("posicao_valor_pago")
        )
    )

    return ranking.sort(
        [
            "ano_emenda",
            "valor_pago",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_relacionamento_autor_favorecido(
    favorecidos: pl.DataFrame,
) -> pl.DataFrame:
    relacionamento = favorecidos.group_by(
        [
            "ano_emenda",
            "codigo_autor_emenda",
            "nome_autor_emenda",
            "codigo_favorecido",
            "favorecido",
            "natureza_juridica",
            "tipo_favorecido",
            "uf_favorecido",
            "municipio_favorecido",
        ]
    ).agg(
        pl.col("codigo_emenda").n_unique().alias("quantidade_emendas"),
        pl.col("valor_recebido").sum().alias("valor_recebido"),
    )

    relacionamento = relacionamento.with_columns(
        pl.col("valor_recebido")
        .sum()
        .over(
            [
                "ano_emenda",
                "codigo_autor_emenda",
            ]
        )
        .alias("total_recebido_por_autor")
    )

    relacionamento = relacionamento.with_columns(
        pl.when(pl.col("total_recebido_por_autor") > 0)
        .then(pl.col("valor_recebido") / pl.col("total_recebido_por_autor") * 100)
        .otherwise(None)
        .round(4)
        .alias("percentual_concentracao_autor")
    )

    relacionamento = relacionamento.with_columns(
        pl.col("valor_recebido")
        .rank(
            method="dense",
            descending=True,
        )
        .over(
            [
                "ano_emenda",
                "codigo_autor_emenda",
            ]
        )
        .cast(pl.Int32)
        .alias("posicao_favorecido_por_autor")
    )

    return relacionamento.sort(
        [
            "ano_emenda",
            "nome_autor_emenda",
            "valor_recebido",
        ],
        descending=[
            False,
            False,
            True,
        ],
    )
