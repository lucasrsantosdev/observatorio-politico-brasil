from __future__ import annotations

import re
import unicodedata

import polars as pl


def normalize_column_name(value: str) -> str:
    normalized = unicodedata.normalize(
        "NFKD",
        value,
    )

    without_accents = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )

    return (
        re.sub(
            r"[^a-zA-Z0-9]+",
            "_",
            without_accents,
        )
        .strip("_")
        .lower()
    )


def normalize_columns(
    dataframe: pl.DataFrame,
) -> pl.DataFrame:
    return dataframe.rename(
        {column: normalize_column_name(column) for column in dataframe.columns}
    )


def clean_text(
    column: str,
) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars().replace("", None)


def clean_code(
    column: str,
) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars().replace("", None)


def parse_brazilian_decimal(
    column: str,
) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .replace("", None)
        .str.replace_all(r"\.", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
    )


def parse_brazilian_date(
    column: str,
) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .replace("", None)
        .str.strptime(
            pl.Date,
            format="%d/%m/%Y",
            strict=False,
        )
    )


def build_licitacao_key() -> pl.Expr:
    return pl.concat_str(
        [
            pl.col("codigo_ug").fill_null("SEM_UG"),
            pl.col("codigo_modalidade_compra").fill_null("SEM_MODALIDADE"),
            pl.col("numero_licitacao").fill_null("SEM_NUMERO"),
        ],
        separator="|",
    ).alias("chave_licitacao")


def add_source_metadata(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    return dataframe.with_columns(
        pl.lit(periodo).alias("periodo_origem"),
        pl.lit(int(periodo[:4])).cast(pl.Int32).alias("ano_origem"),
        pl.lit(int(periodo[4:6])).cast(pl.Int8).alias("mes_origem"),
        pl.lit(source_file).alias("arquivo_origem"),
    )


def transform_licitacoes(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_code("numero_licitacao"),
        clean_code("codigo_ug"),
        clean_text("nome_ug"),
        clean_code("codigo_modalidade_compra"),
        clean_text("modalidade_compra"),
        clean_code("numero_processo"),
        clean_text("objeto"),
        clean_text("situacao_licitacao"),
        clean_code("codigo_orgao_superior"),
        clean_text("nome_orgao_superior"),
        clean_code("codigo_orgao"),
        clean_text("nome_orgao"),
        clean_text("uf").str.to_uppercase(),
        clean_text("municipio").str.to_uppercase(),
        parse_brazilian_date("data_resultado_compra"),
        parse_brazilian_date("data_abertura"),
        parse_brazilian_decimal("valor_licitacao"),
    )

    # Primeiro cria a chave.
    dataframe = dataframe.with_columns(build_licitacao_key())

    # Depois cria os demais campos derivados.
    dataframe = dataframe.with_columns(
        pl.col("objeto")
        .fill_null("")
        .str.to_lowercase()
        .str.contains("sigilo")
        .alias("licitacao_sigilosa"),
        pl.when(
            pl.col("data_resultado_compra").is_not_null()
            & pl.col("data_abertura").is_not_null()
        )
        .then(
            (pl.col("data_resultado_compra") - pl.col("data_abertura")).dt.total_days()
        )
        .otherwise(None)
        .cast(pl.Int32)
        .alias("duracao_processo_dias"),
        (
            pl.col("data_resultado_compra").is_not_null()
            & pl.col("data_abertura").is_not_null()
            & (pl.col("data_resultado_compra") < pl.col("data_abertura"))
        ).alias("data_processo_inconsistente"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_itens_licitacao(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_code("numero_licitacao"),
        clean_code("codigo_ug"),
        clean_text("nome_ug"),
        clean_code("codigo_modalidade_compra"),
        clean_text("modalidade_compra"),
        clean_code("numero_processo"),
        clean_code("codigo_orgao"),
        clean_text("nome_orgao"),
        clean_code("codigo_item_compra"),
        clean_text("descricao"),
        parse_brazilian_decimal("quantidade_item"),
        parse_brazilian_decimal("valor_item"),
        clean_code("codigo_vencedor"),
        clean_text("nome_vencedor"),
    )

    # 1. Chave da licitação.
    dataframe = dataframe.with_columns(build_licitacao_key())

    # 2. Chave lógica do item, compartilhada com participantes.
    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_licitacao"),
                pl.col("codigo_item_compra").fill_null("SEM_ITEM"),
            ],
            separator="|",
        ).alias("chave_item_licitacao")
    )

    # 3. Chave física da linha item/vencedor.
    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_item_licitacao"),
                pl.col("codigo_vencedor").fill_null("SEM_VENCEDOR"),
            ],
            separator="|",
        ).alias("chave_item_vencedor"),
        pl.when(
            pl.col("quantidade_item").is_not_null() & pl.col("valor_item").is_not_null()
        )
        .then(pl.col("quantidade_item") * pl.col("valor_item"))
        .otherwise(None)
        .alias("valor_total_item_calculado"),
        (
            pl.col("codigo_vencedor").is_not_null()
            & ~pl.col("codigo_vencedor").is_in(["-11", "-3", "0"])
        ).alias("vencedor_identificado"),
        pl.col("nome_vencedor")
        .fill_null("")
        .str.to_lowercase()
        .str.contains("sigil")
        .alias("vencedor_sigiloso"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_participantes_licitacao(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_code("numero_licitacao"),
        clean_code("codigo_ug"),
        clean_text("nome_ug"),
        clean_code("codigo_modalidade_compra"),
        clean_text("modalidade_compra"),
        clean_code("numero_processo"),
        clean_code("codigo_orgao"),
        clean_text("nome_orgao"),
        clean_code("codigo_item_compra"),
        clean_text("descricao_item_compra"),
        clean_code("codigo_participante"),
        clean_text("nome_participante"),
        clean_text("flag_vencedor").str.to_uppercase(),
    )

    dataframe = dataframe.with_columns(build_licitacao_key())

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_licitacao"),
                pl.col("codigo_item_compra").fill_null("SEM_ITEM"),
            ],
            separator="|",
        ).alias("chave_item_licitacao")
    )

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_item_licitacao"),
                pl.col("codigo_participante").fill_null("SEM_PARTICIPANTE"),
            ],
            separator="|",
        ).alias("chave_participacao"),
        pl.col("flag_vencedor")
        .is_in(["SIM", "S", "YES"])
        .alias("participante_vencedor"),
        (
            pl.col("codigo_participante").is_not_null()
            & ~pl.col("codigo_participante").is_in(["-11", "-3", "0"])
        ).alias("participante_identificado"),
        pl.col("nome_participante")
        .fill_null("")
        .str.to_lowercase()
        .str.contains("sigil")
        .alias("participante_sigiloso"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_empenhos_relacionados(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_code("numero_licitacao"),
        clean_code("codigo_ug"),
        clean_text("nome_ug"),
        clean_code("codigo_modalidade_compra"),
        clean_text("modalidade_compra"),
        clean_code("numero_processo"),
        clean_code("codigo_empenho"),
        parse_brazilian_date("data_emissao_empenho"),
        clean_text("observacao_empenho"),
        parse_brazilian_decimal("valor_empenho_r"),
    )

    dataframe = dataframe.with_columns(build_licitacao_key())

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_licitacao"),
                pl.col("codigo_empenho").fill_null("SEM_EMPENHO"),
            ],
            separator="|",
        ).alias("chave_empenho_licitacao")
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )
