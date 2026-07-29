from __future__ import annotations

from datetime import date

import polars as pl


def build_dim_convenente(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.with_columns(pl.col("convenente").fill_null("NÃO INFORMADO"))
        .group_by("convenente")
        .agg(
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
            pl.col("valor_convenio").sum().alias("valor_total_convenios"),
        )
        .with_row_index(
            name="chave_convenente",
            offset=1,
        )
        .with_columns(pl.col("chave_convenente").cast(pl.Int32))
        .select(
            "chave_convenente",
            "convenente",
            "quantidade_convenios",
            "valor_total_convenios",
        )
        .sort("convenente")
    )


def build_dim_funcao(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(
            "codigo_funcao",
            "nome_funcao",
        )
        .unique()
        .with_columns(pl.col("codigo_funcao").alias("chave_funcao"))
        .select(
            "chave_funcao",
            "codigo_funcao",
            "nome_funcao",
        )
        .sort("codigo_funcao")
    )


def build_dim_subfuncao(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(
            "codigo_funcao",
            "codigo_subfuncao",
            "nome_subfuncao",
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col("codigo_funcao").fill_null(""),
                    pl.col("codigo_subfuncao").fill_null(""),
                ],
                separator="|",
            ).alias("chave_subfuncao"),
            pl.col("codigo_funcao").alias("chave_funcao"),
        )
        .select(
            "chave_subfuncao",
            "chave_funcao",
            "codigo_funcao",
            "codigo_subfuncao",
            "nome_subfuncao",
        )
        .sort(
            [
                "codigo_funcao",
                "codigo_subfuncao",
            ]
        )
    )


def build_dim_localidade(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(pl.col("localidade_gasto").fill_null("NÃO INFORMADA"))
        .unique()
        .with_row_index(
            name="chave_localidade",
            offset=1,
        )
        .with_columns(pl.col("chave_localidade").cast(pl.Int32))
        .sort("localidade_gasto")
    )


def build_dim_tipo_emenda(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(pl.col("tipo_emenda").fill_null("NÃO INFORMADO"))
        .unique()
        .with_row_index(
            name="chave_tipo_emenda",
            offset=1,
        )
        .with_columns(pl.col("chave_tipo_emenda").cast(pl.Int32))
        .sort("tipo_emenda")
    )


def build_dim_emenda(
    relacionamento: pl.DataFrame,
) -> pl.DataFrame:
    return (
        relacionamento.group_by("codigo_emenda")
        .agg(
            pl.col("ano_emenda").min().alias("ano_emenda"),
            pl.col("tipo_emenda").drop_nulls().first().alias("tipo_emenda"),
            pl.col("numero_convenio").n_unique().alias("quantidade_convenios"),
        )
        .with_columns(pl.col("codigo_emenda").alias("chave_emenda"))
        .select(
            "chave_emenda",
            "codigo_emenda",
            "ano_emenda",
            "tipo_emenda",
            "quantidade_convenios",
        )
        .sort("codigo_emenda")
    )


def build_dim_tempo_convenios(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    datas = fato["data_publicacao_convenio"].drop_nulls()

    if datas.len() == 0:
        return pl.DataFrame(
            schema={
                "chave_data": pl.Int32,
                "data": pl.Date,
                "ano": pl.Int32,
                "mes": pl.Int8,
                "nome_mes": pl.String,
                "dia": pl.Int8,
                "trimestre": pl.Int8,
                "periodo": pl.String,
            }
        )

    data_inicial = datas.min()
    data_final = datas.max()

    if not isinstance(
        data_inicial,
        date,
    ):
        raise TypeError("Data inicial inválida.")

    if not isinstance(
        data_final,
        date,
    ):
        raise TypeError("Data final inválida.")

    return (
        pl.DataFrame(
            {
                "data": pl.date_range(
                    data_inicial,
                    data_final,
                    interval="1d",
                    closed="both",
                    eager=True,
                )
            }
        )
        .with_columns(
            pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("chave_data"),
            pl.col("data").dt.year().alias("ano"),
            pl.col("data").dt.month().cast(pl.Int8).alias("mes"),
            pl.col("data").dt.day().cast(pl.Int8).alias("dia"),
            pl.col("data").dt.quarter().cast(pl.Int8).alias("trimestre"),
            pl.col("data").dt.strftime("%Y%m").alias("periodo"),
        )
        .with_columns(
            pl.when(pl.col("mes") == 1)
            .then(pl.lit("Janeiro"))
            .when(pl.col("mes") == 2)
            .then(pl.lit("Fevereiro"))
            .when(pl.col("mes") == 3)
            .then(pl.lit("Março"))
            .when(pl.col("mes") == 4)
            .then(pl.lit("Abril"))
            .when(pl.col("mes") == 5)
            .then(pl.lit("Maio"))
            .when(pl.col("mes") == 6)
            .then(pl.lit("Junho"))
            .when(pl.col("mes") == 7)
            .then(pl.lit("Julho"))
            .when(pl.col("mes") == 8)
            .then(pl.lit("Agosto"))
            .when(pl.col("mes") == 9)
            .then(pl.lit("Setembro"))
            .when(pl.col("mes") == 10)
            .then(pl.lit("Outubro"))
            .when(pl.col("mes") == 11)
            .then(pl.lit("Novembro"))
            .otherwise(pl.lit("Dezembro"))
            .alias("nome_mes")
        )
        .select(
            "chave_data",
            "data",
            "ano",
            "mes",
            "nome_mes",
            "dia",
            "trimestre",
            "periodo",
        )
        .sort("data")
    )
