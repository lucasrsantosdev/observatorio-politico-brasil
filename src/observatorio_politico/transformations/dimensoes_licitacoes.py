from __future__ import annotations

from datetime import date

import polars as pl


def build_dim_orgao(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        licitacoes.select(
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
        )
        .unique()
        .sort(
            [
                "codigo_orgao_superior",
                "codigo_orgao",
                "nome_orgao_superior",
                "nome_orgao",
            ]
        )
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
    )


def build_dim_unidade_gestora(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        licitacoes.select(
            "codigo_orgao",
            "codigo_ug",
            "nome_ug",
        )
        .unique()
        .sort(
            [
                "codigo_orgao",
                "codigo_ug",
                "nome_ug",
            ]
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("codigo_orgao").fill_null(""),
                    pl.col("codigo_ug").fill_null(""),
                ],
                separator="|",
            ).alias("chave_unidade_gestora")
        )
        .select(
            "chave_unidade_gestora",
            "codigo_orgao",
            "codigo_ug",
            "nome_ug",
        )
    )


def build_dim_fornecedor(
    itens: pl.DataFrame,
) -> pl.DataFrame:
    return (
        itens.select(
            "codigo_vencedor",
            "nome_vencedor",
        )
        .unique()
        .sort(
            [
                "codigo_vencedor",
                "nome_vencedor",
            ]
        )
        .with_columns(pl.col("codigo_vencedor").alias("chave_fornecedor"))
        .select(
            "chave_fornecedor",
            "codigo_vencedor",
            "nome_vencedor",
        )
    )


def build_dim_modalidade(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        licitacoes.select(
            "codigo_modalidade_compra",
            "modalidade_compra",
        )
        .unique()
        .sort(
            [
                "codigo_modalidade_compra",
                "modalidade_compra",
            ]
        )
        .with_columns(pl.col("codigo_modalidade_compra").alias("chave_modalidade"))
        .select(
            "chave_modalidade",
            "codigo_modalidade_compra",
            "modalidade_compra",
        )
    )


def build_dim_situacao(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        licitacoes.select(pl.col("situacao_licitacao").fill_null("NÃO INFORMADA"))
        .unique()
        .sort("situacao_licitacao")
        .with_row_index(
            name="chave_situacao",
            offset=1,
        )
        .with_columns(pl.col("chave_situacao").cast(pl.Int32))
        .select(
            "chave_situacao",
            "situacao_licitacao",
        )
    )


def build_dim_localidade(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        licitacoes.select(
            pl.col("uf").fill_null("NÃO INFORMADA"),
            pl.col("municipio").fill_null("NÃO INFORMADO"),
        )
        .unique()
        .sort(
            [
                "uf",
                "municipio",
            ]
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("uf"),
                    pl.col("municipio"),
                ],
                separator="|",
            ).alias("chave_localidade")
        )
        .select(
            "chave_localidade",
            "uf",
            "municipio",
        )
    )


def build_dim_tempo(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    datas = pl.concat(
        [
            licitacoes["data_abertura"].drop_nulls(),
            licitacoes["data_resultado_compra"].drop_nulls(),
        ]
    )

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
