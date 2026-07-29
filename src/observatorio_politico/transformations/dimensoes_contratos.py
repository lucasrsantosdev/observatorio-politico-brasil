from __future__ import annotations

from datetime import date

import polars as pl


def build_dim_orgao(
    contratos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos.select(
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col("codigo_orgao_superior").fill_null(""),
                    pl.col("codigo_orgao").fill_null(""),
                ],
                separator="|",
            ).alias("chave_orgao")
        )
        .select(
            "chave_orgao",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
        )
        .sort(
            [
                "nome_orgao_superior",
                "nome_orgao",
            ]
        )
    )


def build_dim_unidade_gestora(
    contratos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos.select(
            "codigo_orgao_superior",
            "codigo_orgao",
            "codigo_ug",
            "nome_ug",
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [
                    pl.col("codigo_orgao_superior").fill_null(""),
                    pl.col("codigo_orgao").fill_null(""),
                ],
                separator="|",
            ).alias("chave_orgao"),
            pl.concat_str(
                [
                    pl.col("codigo_orgao").fill_null(""),
                    pl.col("codigo_ug").fill_null(""),
                ],
                separator="|",
            ).alias("chave_unidade_gestora"),
        )
        .select(
            "chave_unidade_gestora",
            "chave_orgao",
            "codigo_orgao_superior",
            "codigo_orgao",
            "codigo_ug",
            "nome_ug",
        )
        .sort(
            [
                "codigo_orgao",
                "nome_ug",
            ]
        )
    )


def build_dim_contratado(
    contratos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos.filter(pl.col("codigo_contratado").is_not_null())
        .group_by("codigo_contratado")
        .agg(
            pl.col("nome_contratado")
            .drop_nulls()
            .sort()
            .last()
            .alias("nome_contratado"),
            pl.col("nome_contratado")
            .drop_nulls()
            .n_unique()
            .cast(pl.Int32)
            .alias("quantidade_nomes_encontrados"),
            pl.col("chave_registro_contrato").n_unique().alias("quantidade_contratos"),
            pl.col("codigo_orgao").drop_nulls().n_unique().alias("quantidade_orgaos"),
            pl.col("valor_final_compra").sum().alias("valor_final_total"),
        )
        .with_columns(
            pl.col("codigo_contratado").alias("chave_contratado"),
            (pl.col("quantidade_nomes_encontrados") > 1).alias("possui_variacao_nome"),
        )
        .select(
            "chave_contratado",
            "codigo_contratado",
            "nome_contratado",
            "quantidade_nomes_encontrados",
            "possui_variacao_nome",
            "quantidade_contratos",
            "quantidade_orgaos",
            "valor_final_total",
        )
        .sort("nome_contratado")
    )


def build_dim_modalidade_compra(
    contratos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos.select(pl.col("modalidade_compra").fill_null("NÃO INFORMADA"))
        .unique()
        .with_row_index(
            name="codigo_modalidade",
            offset=1,
        )
        .with_columns(pl.col("codigo_modalidade").cast(pl.Int32))
        .sort("modalidade_compra")
    )


def build_dim_situacao_contrato(
    contratos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos.select(pl.col("situacao_contrato").fill_null("NÃO INFORMADA"))
        .unique()
        .with_row_index(
            name="codigo_situacao",
            offset=1,
        )
        .with_columns(pl.col("codigo_situacao").cast(pl.Int32))
        .sort("situacao_contrato")
    )


def build_dim_tempo(
    *,
    first_period: str,
    last_period: str,
) -> pl.DataFrame:
    primeira_data = date(
        int(first_period[:4]),
        int(first_period[4:]),
        1,
    )

    ultimo_ano = int(last_period[:4])
    ultimo_mes = int(last_period[4:])

    if ultimo_mes == 12:
        limite = date(
            ultimo_ano + 1,
            1,
            1,
        )
    else:
        limite = date(
            ultimo_ano,
            ultimo_mes + 1,
            1,
        )

    quantidade_dias = (limite - primeira_data).days

    return (
        pl.DataFrame(
            {
                "data": pl.date_range(
                    primeira_data,
                    limite,
                    interval="1d",
                    closed="left",
                    eager=True,
                )
            }
        )
        .with_columns(
            pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("chave_data"),
            pl.col("data").dt.year().alias("ano"),
            pl.col("data").dt.month().alias("mes"),
            pl.col("data").dt.day().alias("dia"),
            pl.col("data").dt.quarter().alias("trimestre"),
            pl.col("data").dt.week().alias("semana_ano"),
            pl.col("data").dt.weekday().alias("dia_semana_numero"),
            pl.col("data").dt.strftime("%Y%m").alias("periodo"),
            pl.col("data").dt.strftime("%m/%Y").alias("mes_ano"),
        )
        .with_columns(
            pl.when(pl.col("mes").is_in([1, 2, 3]))
            .then(pl.lit("1º trimestre"))
            .when(pl.col("mes").is_in([4, 5, 6]))
            .then(pl.lit("2º trimestre"))
            .when(pl.col("mes").is_in([7, 8, 9]))
            .then(pl.lit("3º trimestre"))
            .otherwise(pl.lit("4º trimestre"))
            .alias("nome_trimestre"),
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
            .alias("nome_mes"),
            (pl.col("dia_semana_numero").is_in([6, 7])).alias("fim_de_semana"),
        )
        .select(
            "chave_data",
            "data",
            "ano",
            "mes",
            "nome_mes",
            "dia",
            "trimestre",
            "nome_trimestre",
            "semana_ano",
            "dia_semana_numero",
            "periodo",
            "mes_ano",
            "fim_de_semana",
        )
        .head(quantidade_dias)
        .sort("data")
    )
