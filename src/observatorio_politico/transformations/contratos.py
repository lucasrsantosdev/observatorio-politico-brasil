from __future__ import annotations

import hashlib
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


def clean_string(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.String).str.strip_chars().replace("", None)


def parse_date(column: str) -> pl.Expr:
    return clean_string(column).str.strptime(
        pl.Date,
        format="%d/%m/%Y",
        strict=False,
    )


def parse_decimal(column: str) -> pl.Expr:
    return (
        clean_string(column)
        .str.replace_all(r"\.", "")
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
    )


def build_content_hash(
    dataframe: pl.DataFrame,
    columns: list[str],
    alias: str,
) -> pl.DataFrame:
    available_columns = [column for column in columns if column in dataframe.columns]

    if not available_columns:
        return dataframe.with_columns(
            pl.lit(hashlib.sha256(b"").hexdigest()).alias(alias)
        )

    return dataframe.with_columns(
        pl.concat_str(
            [
                pl.col(column).cast(pl.String).fill_null("")
                for column in available_columns
            ],
            separator="|",
        )
        .hash(seed=42)
        .cast(pl.UInt64)
        .cast(pl.String)
        .alias(alias)
    )


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


def add_contract_group_key(
    dataframe: pl.DataFrame,
) -> pl.DataFrame:
    return dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("codigo_ug").fill_null("SEM_UG"),
                pl.col("numero_contrato").fill_null("SEM_CONTRATO"),
            ],
            separator="|",
        ).alias("chave_grupo_contrato")
    )


def transform_contratos(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.rename(
        {
            "numero_do_contrato": "numero_contrato",
        }
    )

    dataframe = dataframe.with_columns(
        clean_string("numero_contrato"),
        clean_string("objeto"),
        clean_string("fundamento_legal"),
        clean_string("modalidade_compra"),
        clean_string("situacao_contrato"),
        clean_string("codigo_orgao_superior"),
        clean_string("nome_orgao_superior"),
        clean_string("codigo_orgao"),
        clean_string("nome_orgao"),
        clean_string("codigo_ug"),
        clean_string("nome_ug"),
        parse_date("data_assinatura_contrato"),
        parse_date("data_publicacao_dou"),
        parse_date("data_inicio_vigencia"),
        parse_date("data_fim_vigencia"),
        clean_string("codigo_contratado"),
        clean_string("nome_contratado"),
        parse_decimal("valor_inicial_compra"),
        parse_decimal("valor_final_compra"),
        clean_string("numero_licitacao"),
        clean_string("codigo_ug_licitacao"),
        clean_string("nome_ug_licitacao"),
        clean_string("codigo_modalidade_compra_licitacao"),
        clean_string("modalidade_compra_licitacao"),
    )

    dataframe = add_contract_group_key(dataframe)

    dataframe = build_content_hash(
        dataframe,
        columns=[
            "objeto",
            "fundamento_legal",
            "modalidade_compra",
            "situacao_contrato",
            "codigo_orgao_superior",
            "codigo_orgao",
            "codigo_ug",
            "data_assinatura_contrato",
            "data_publicacao_dou",
            "data_inicio_vigencia",
            "data_fim_vigencia",
            "codigo_contratado",
            "valor_inicial_compra",
            "valor_final_compra",
            "numero_licitacao",
            "codigo_ug_licitacao",
            ("codigo_modalidade_compra_licitacao"),
        ],
        alias="hash_conteudo_contrato",
    )

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("hash_conteudo_contrato"),
            ],
            separator="|",
        ).alias("chave_registro_contrato"),
        (pl.col("valor_final_compra") - pl.col("valor_inicial_compra")).alias(
            "variacao_valor_contrato"
        ),
        pl.when(pl.col("valor_inicial_compra") > 0)
        .then(
            (pl.col("valor_final_compra") - pl.col("valor_inicial_compra"))
            / pl.col("valor_inicial_compra")
            * 100
        )
        .otherwise(None)
        .round(4)
        .alias("percentual_variacao_valor"),
        pl.when(
            pl.col("data_inicio_vigencia").is_not_null()
            & pl.col("data_fim_vigencia").is_not_null()
        )
        .then(
            (
                pl.col("data_fim_vigencia") - pl.col("data_inicio_vigencia")
            ).dt.total_days()
        )
        .otherwise(None)
        .cast(pl.Int32)
        .alias("duracao_vigencia_dias"),
        (
            pl.col("objeto").fill_null("").str.to_lowercase().str.contains("sigilo")
            | pl.col("nome_contratado")
            .fill_null("")
            .str.to_lowercase()
            .str.contains("sigil")
            | pl.col("codigo_contratado").is_in(["-11", "-3"])
        ).alias("contrato_sigiloso"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_itens_contrato(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_string("codigo_orgao"),
        clean_string("nome_orgao"),
        clean_string("codigo_ug"),
        clean_string("nome_ug"),
        clean_string("numero_contrato"),
        clean_string("codigo_item_compra"),
        clean_string("descricao_item_compra"),
        clean_string("descricao_complementar_item_compra"),
        parse_decimal("quantidade_item"),
        parse_decimal("valor_item"),
    )

    dataframe = add_contract_group_key(dataframe)

    dataframe = build_content_hash(
        dataframe,
        columns=[
            "codigo_item_compra",
            "descricao_item_compra",
            ("descricao_complementar_item_compra"),
            "quantidade_item",
            "valor_item",
        ],
        alias="hash_conteudo_item",
    )

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("codigo_item_compra").fill_null("SEM_ITEM"),
                pl.col("hash_conteudo_item"),
            ],
            separator="|",
        ).alias("chave_item_contrato"),
        (pl.col("quantidade_item") * pl.col("valor_item")).alias(
            "valor_total_item_calculado"
        ),
        (pl.col("codigo_item_compra") == "0").alias("item_codigo_zero"),
        (
            pl.col("descricao_item_compra")
            .fill_null("")
            .str.to_lowercase()
            .str.contains("sigilo")
            | pl.col("descricao_complementar_item_compra")
            .fill_null("")
            .str.to_lowercase()
            .str.contains("sigilo")
        ).alias("item_sigiloso"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_termos_aditivos(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    dataframe = dataframe.with_columns(
        clean_string("numero_contrato"),
        clean_string("codigo_orgao_superior"),
        clean_string("nome_orgao_superior"),
        clean_string("codigo_orgao"),
        clean_string("nome_orgao"),
        clean_string("codigo_ug"),
        clean_string("nome_ug"),
        clean_string("numero_termo_aditivo"),
        parse_date("data_publicacao"),
        clean_string("objeto"),
    )

    dataframe = add_contract_group_key(dataframe)

    dataframe = build_content_hash(
        dataframe,
        columns=[
            "numero_termo_aditivo",
            "data_publicacao",
            "objeto",
        ],
        alias="hash_conteudo_termo",
    )

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("numero_termo_aditivo").fill_null("SEM_TERMO"),
                pl.col("hash_conteudo_termo"),
            ],
            separator="|",
        ).alias("chave_termo_aditivo"),
        (
            pl.col("objeto").fill_null("").str.to_lowercase().str.contains("prorroga")
        ).alias("termo_prorrogacao"),
        (
            pl.col("objeto")
            .fill_null("")
            .str.to_lowercase()
            .str.contains("reajuste|repactua|revisao|revisão")
        ).alias("termo_reajuste"),
        (
            pl.col("objeto")
            .fill_null("")
            .str.to_lowercase()
            .str.contains("acrescimo|acréscimo|supressao|supressão")
        ).alias("termo_alteracao_quantitativa"),
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )


def transform_apostilamentos(
    dataframe: pl.DataFrame,
    *,
    periodo: str,
    source_file: str,
) -> pl.DataFrame:
    dataframe = normalize_columns(dataframe)

    expected_columns = {
        "numero_contrato": pl.String,
        "codigo_orgao_superior": pl.String,
        "nome_orgao_superior": pl.String,
        "codigo_orgao": pl.String,
        "nome_orgao": pl.String,
        "codigo_ug": pl.String,
        "nome_ug": pl.String,
        "numero_apostilamento": pl.String,
        "descricao_apostilamento": pl.String,
        "valor_apostilamento": pl.Float64,
        "data_de_inclusao": pl.Date,
        "situacao_apostilamento": pl.String,
    }

    for column, dtype in expected_columns.items():
        if column not in dataframe.columns:
            dataframe = dataframe.with_columns(pl.lit(None).cast(dtype).alias(column))

    if dataframe.height > 0:
        dataframe = dataframe.with_columns(
            clean_string("numero_contrato"),
            clean_string("codigo_orgao_superior"),
            clean_string("nome_orgao_superior"),
            clean_string("codigo_orgao"),
            clean_string("nome_orgao"),
            clean_string("codigo_ug"),
            clean_string("nome_ug"),
            clean_string("numero_apostilamento"),
            clean_string("descricao_apostilamento"),
            parse_decimal("valor_apostilamento"),
            parse_date("data_de_inclusao"),
            clean_string("situacao_apostilamento"),
        )

    dataframe = add_contract_group_key(dataframe)

    dataframe = build_content_hash(
        dataframe,
        columns=[
            "numero_apostilamento",
            "descricao_apostilamento",
            "valor_apostilamento",
            "data_de_inclusao",
            "situacao_apostilamento",
        ],
        alias="hash_conteudo_apostilamento",
    )

    dataframe = dataframe.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("numero_apostilamento").fill_null("SEM_APOSTILAMENTO"),
                pl.col("hash_conteudo_apostilamento"),
            ],
            separator="|",
        ).alias("chave_apostilamento")
    )

    return add_source_metadata(
        dataframe,
        periodo=periodo,
        source_file=source_file,
    )
