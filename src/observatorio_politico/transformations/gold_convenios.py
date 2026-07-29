from __future__ import annotations

import polars as pl


def build_fato_convenios(
    convenios: pl.DataFrame,
) -> pl.DataFrame:
    return (
        convenios.group_by("numero_convenio")
        .agg(
            pl.col("convenente").drop_nulls().first().alias("convenente"),
            pl.col("objeto_convenio").drop_nulls().first().alias("objeto_convenio"),
            pl.col("data_publicacao_convenio").min().alias("data_publicacao_convenio"),
            pl.col("valor_convenio").first().alias("valor_convenio"),
            pl.col("codigo_funcao").drop_nulls().first().alias("codigo_funcao"),
            pl.col("nome_funcao").drop_nulls().first().alias("nome_funcao"),
            pl.col("codigo_subfuncao").drop_nulls().first().alias("codigo_subfuncao"),
            pl.col("nome_subfuncao").drop_nulls().first().alias("nome_subfuncao"),
            pl.col("localidade_gasto").drop_nulls().first().alias("localidade_gasto"),
            pl.col("tipo_emenda").drop_nulls().first().alias("tipo_emenda"),
            pl.col("codigo_emenda")
            .n_unique()
            .cast(pl.Int32)
            .alias("quantidade_emendas"),
            pl.col("codigo_emenda")
            .sort()
            .first()
            .alias("codigo_emenda_representativa"),
            pl.col("ano_emenda").min().alias("primeiro_ano_emenda"),
            pl.col("ano_emenda").max().alias("ultimo_ano_emenda"),
            pl.col("ano_emenda")
            .n_unique()
            .cast(pl.Int32)
            .alias("quantidade_anos_emenda"),
        )
        .with_columns(
            pl.col("numero_convenio").alias("chave_convenio"),
            (pl.col("quantidade_emendas") > 1).alias("possui_multiplas_emendas"),
        )
        .select(
            "chave_convenio",
            "numero_convenio",
            "convenente",
            "objeto_convenio",
            "data_publicacao_convenio",
            "valor_convenio",
            "codigo_funcao",
            "nome_funcao",
            "codigo_subfuncao",
            "nome_subfuncao",
            "localidade_gasto",
            "tipo_emenda",
            "quantidade_emendas",
            "codigo_emenda_representativa",
            "primeiro_ano_emenda",
            "ultimo_ano_emenda",
            "quantidade_anos_emenda",
            "possui_multiplas_emendas",
        )
        .sort(
            "valor_convenio",
            descending=True,
        )
    )


def build_relacionamento_emenda_convenio(
    convenios: pl.DataFrame,
) -> pl.DataFrame:
    return (
        convenios.select(
            "codigo_emenda",
            "numero_convenio",
            "ano_emenda",
            "tipo_emenda",
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col("codigo_emenda"),
                    pl.col("numero_convenio"),
                ],
                separator="|",
            ).alias("chave_emenda_convenio"),
            pl.col("numero_convenio").alias("chave_convenio"),
        )
        .select(
            "chave_emenda_convenio",
            "chave_convenio",
            "codigo_emenda",
            "numero_convenio",
            "ano_emenda",
            "tipo_emenda",
        )
        .sort(
            [
                "codigo_emenda",
                "numero_convenio",
            ]
        )
    )


def build_ranking_convenentes(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.with_columns(pl.col("convenente").fill_null("NÃO INFORMADO"))
        .group_by("convenente")
        .agg(
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
            pl.col("valor_convenio").sum().alias("valor_total_convenios"),
            pl.col("valor_convenio").mean().round(2).alias("valor_medio_convenio"),
            pl.col("valor_convenio").max().alias("maior_valor_convenio"),
            pl.col("codigo_emenda_representativa")
            .n_unique()
            .alias("quantidade_emendas_representativas"),
        )
        .with_columns(
            pl.col("valor_total_convenios")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor")
        )
        .sort(
            "valor_total_convenios",
            descending=True,
        )
    )


def build_ranking_funcoes(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by(
            [
                "codigo_funcao",
                "nome_funcao",
            ]
        )
        .agg(
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
            pl.col("valor_convenio").sum().alias("valor_total_convenios"),
            pl.col("convenente")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_convenentes"),
            pl.col("valor_convenio").mean().round(2).alias("valor_medio_convenio"),
        )
        .with_columns(
            pl.col("valor_total_convenios")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor")
        )
        .sort(
            "valor_total_convenios",
            descending=True,
        )
    )


def build_ranking_localidades(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.with_columns(pl.col("localidade_gasto").fill_null("NÃO INFORMADA"))
        .group_by("localidade_gasto")
        .agg(
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
            pl.col("valor_convenio").sum().alias("valor_total_convenios"),
            pl.col("convenente")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_convenentes"),
        )
        .with_columns(
            pl.col("valor_total_convenios")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor")
        )
        .sort(
            "valor_total_convenios",
            descending=True,
        )
    )


def build_resumo_anual(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by("primeiro_ano_emenda")
        .agg(
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
            pl.col("valor_convenio").sum().alias("valor_total_convenios"),
            pl.col("convenente")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_convenentes"),
            pl.col("valor_convenio").mean().round(2).alias("valor_medio_convenio"),
        )
        .rename(
            {
                "primeiro_ano_emenda": "ano_emenda",
            }
        )
        .sort("ano_emenda")
    )
