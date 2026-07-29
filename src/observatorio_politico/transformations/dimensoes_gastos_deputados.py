from __future__ import annotations

from datetime import date

import polars as pl


def build_dim_beneficiario(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.group_by(
            [
                "chave_beneficiario",
                "codigo_beneficiario",
                "nome_beneficiario",
                "tipo_beneficiario",
                "partido",
                "uf",
            ]
        )
        .agg(
            pl.len().alias("quantidade_lancamentos"),
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
        )
        .sort("nome_beneficiario")
    )


def build_dim_fornecedor(
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
            pl.col("valor_liquido").sum().alias("valor_total_liquido"),
        )
        .sort("nome_fornecedor")
    )


def build_dim_tipo_despesa(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(
            "codigo_tipo_despesa",
            "tipo_despesa",
        )
        .unique()
        .sort("codigo_tipo_despesa")
        .with_columns(
            pl.col("codigo_tipo_despesa").cast(pl.String).alias("chave_tipo_despesa")
        )
        .select(
            "chave_tipo_despesa",
            "codigo_tipo_despesa",
            "tipo_despesa",
        )
    )


def build_dim_partido(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(pl.col("partido").fill_null("NÃO INFORMADO"))
        .unique()
        .sort("partido")
        .with_columns(pl.col("partido").alias("chave_partido"))
        .select(
            "chave_partido",
            "partido",
        )
    )


def build_dim_uf(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select(pl.col("uf").fill_null("NÃO INFORMADA"))
        .unique()
        .sort("uf")
        .with_columns(pl.col("uf").alias("chave_uf"))
        .select(
            "chave_uf",
            "uf",
        )
    )


def build_dim_movimento(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    return (
        fato.select("tipo_movimento")
        .unique()
        .sort("tipo_movimento")
        .with_columns(pl.col("tipo_movimento").alias("chave_movimento"))
        .select(
            "chave_movimento",
            "tipo_movimento",
        )
    )


def build_dim_tempo(
    fato: pl.DataFrame,
) -> pl.DataFrame:
    datas = fato["data_emissao"].drop_nulls()

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

    inicio = datas.min()
    fim = datas.max()

    if not isinstance(inicio, date):
        raise TypeError("Data inicial inválida.")

    if not isinstance(fim, date):
        raise TypeError("Data final inválida.")

    return (
        pl.DataFrame(
            {
                "data": pl.date_range(
                    inicio,
                    fim,
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
