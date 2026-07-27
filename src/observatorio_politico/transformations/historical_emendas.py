from __future__ import annotations

import logging
import shutil
import unicodedata
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


MONEY_COLUMNS = {
    "emendas": [
        "valor_empenhado",
        "valor_liquidado",
        "valor_pago",
        "valor_restos_pagar_inscritos",
        "valor_restos_pagar_cancelados",
        "valor_restos_pagar_pagos",
    ],
    "convenios": [
        "valor_convenio",
    ],
    "favorecidos": [
        "valor_recebido",
    ],
}


COLUMN_MAPPING = {
    "emendas": {
        "Código da Emenda": "codigo_emenda",
        "Ano da Emenda": "ano_emenda",
        "Tipo de Emenda": "tipo_emenda",
        "Código do Autor da Emenda": "codigo_autor_emenda",
        "Nome do Autor da Emenda": "nome_autor_emenda",
        "Número da emenda": "numero_emenda",
        "Localidade de aplicação do recurso": "localidade_aplicacao",
        "Código Município IBGE": "codigo_municipio_ibge",
        "Município": "municipio",
        "Código UF IBGE": "codigo_uf_ibge",
        "UF": "uf",
        "Região": "regiao",
        "Código Função": "codigo_funcao",
        "Nome Função": "nome_funcao",
        "Código Subfunção": "codigo_subfuncao",
        "Nome Subfunção": "nome_subfuncao",
        "Código Programa": "codigo_programa",
        "Nome Programa": "nome_programa",
        "Código Ação": "codigo_acao",
        "Nome Ação": "nome_acao",
        "Código Plano Orçamentário": "codigo_plano_orcamentario",
        "Nome Plano Orçamentário": "nome_plano_orcamentario",
        "Valor Empenhado": "valor_empenhado",
        "Valor Liquidado": "valor_liquidado",
        "Valor Pago": "valor_pago",
        "Valor Restos A Pagar Inscritos": "valor_restos_pagar_inscritos",
        "Valor Restos A Pagar Cancelados": "valor_restos_pagar_cancelados",
        "Valor Restos A Pagar Pagos": "valor_restos_pagar_pagos",
    },
    "convenios": {
        "Código da Emenda": "codigo_emenda",
        "Código Função": "codigo_funcao",
        "Nome Função": "nome_funcao",
        "Código Subfunção": "codigo_subfuncao",
        "Nome Subfunção": "nome_subfuncao",
        "Localidade do gasto": "localidade_gasto",
        "Tipo de Emenda": "tipo_emenda",
        "Data Publicação Convênio": "data_publicacao_convenio",
        "Convenente": "convenente",
        "Objeto Convênio": "objeto_convenio",
        "Número Convênio": "numero_convenio",
        "Valor Convênio": "valor_convenio",
    },
    "favorecidos": {
        "Código da Emenda": "codigo_emenda",
        "Código do Autor da Emenda": "codigo_autor_emenda",
        "Nome do Autor da Emenda": "nome_autor_emenda",
        "Número da emenda": "numero_emenda",
        "Tipo de Emenda": "tipo_emenda",
        "Ano/Mês": "ano_mes",
        "Código do Favorecido": "codigo_favorecido",
        "Favorecido": "favorecido",
        "Natureza Jurídica": "natureza_juridica",
        "Tipo Favorecido": "tipo_favorecido",
        "UF Favorecido": "uf_favorecido",
        "Município Favorecido": "municipio_favorecido",
        "Valor Recebido": "valor_recebido",
    },
}


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    return "".join(
        character for character in value if not unicodedata.combining(character)
    )


def convert_cp1252_to_utf8(
    source: Path,
    destination: Path,
) -> Path:
    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with (
        source.open(
            "r",
            encoding="cp1252",
            errors="strict",
            newline="",
        ) as input_file,
        destination.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as output_file,
    ):
        shutil.copyfileobj(
            input_file,
            output_file,
            length=1024 * 1024,
        )

    logger.info(
        "Arquivo convertido para UTF-8: %s",
        destination,
    )

    return destination


def _money_expression(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".", literal=True)
        .cast(pl.Float64, strict=False)
        .fill_null(0.0)
        .alias(column)
    )


def _read_csv(
    file_path: Path,
    *,
    entity: str,
) -> pl.LazyFrame:
    mapping = COLUMN_MAPPING[entity]

    dataframe = pl.scan_csv(
        file_path,
        separator=";",
        quote_char='"',
        encoding="utf8",
        infer_schema=False,
        schema_overrides={column: pl.String for column in mapping},
        null_values=[
            "",
            "Sem informação",
            "S/I",
        ],
        truncate_ragged_lines=False,
    )

    available_mapping = {
        source: destination
        for source, destination in mapping.items()
        if source in dataframe.collect_schema().names()
    }

    missing_columns = set(mapping).difference(available_mapping)

    if missing_columns:
        raise ValueError(f"Colunas ausentes em {entity}: {sorted(missing_columns)}")

    return dataframe.rename(available_mapping)


def transform_emendas(
    file_path: Path,
    *,
    years: list[int],
) -> pl.DataFrame:
    dataframe = _read_csv(
        file_path,
        entity="emendas",
    )

    dataframe = dataframe.with_columns(
        pl.col("ano_emenda").cast(pl.Int32, strict=False),
        *[_money_expression(column) for column in MONEY_COLUMNS["emendas"]],
    )

    dataframe = dataframe.filter(pl.col("ano_emenda").is_in(years))

    dataframe = dataframe.with_columns(
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
        pl.when(pl.col("valor_empenhado") > 0)
        .then(pl.col("valor_restos_pagar_pagos") / pl.col("valor_empenhado") * 100)
        .otherwise(None)
        .round(4)
        .alias("percentual_restos_pagos"),
    )

    return (
        dataframe.unique(
            subset=[
                "codigo_emenda",
                "codigo_municipio_ibge",
                "codigo_funcao",
                "codigo_subfuncao",
                "codigo_programa",
                "codigo_acao",
                "codigo_plano_orcamentario",
            ],
            keep="last",
        )
        .sort(
            [
                "ano_emenda",
                "nome_autor_emenda",
                "codigo_emenda",
            ]
        )
        .collect()
    )


def transform_convenios(
    file_path: Path,
    *,
    years: list[int],
) -> pl.DataFrame:
    dataframe = _read_csv(
        file_path,
        entity="convenios",
    )

    dataframe = dataframe.with_columns(
        pl.col("codigo_emenda")
        .str.slice(0, 4)
        .cast(pl.Int32, strict=False)
        .alias("ano_emenda"),
        _money_expression("valor_convenio"),
        pl.col("data_publicacao_convenio").str.strptime(
            pl.Date,
            format="%d/%m/%Y",
            strict=False,
        ),
    )

    return (
        dataframe.filter(pl.col("ano_emenda").is_in(years))
        .unique(
            subset=[
                "codigo_emenda",
                "numero_convenio",
            ],
            keep="last",
        )
        .sort(
            [
                "ano_emenda",
                "codigo_emenda",
                "numero_convenio",
            ]
        )
        .collect()
    )


def transform_favorecidos(
    file_path: Path,
    *,
    years: list[int],
) -> pl.DataFrame:
    dataframe = _read_csv(
        file_path,
        entity="favorecidos",
    )

    dataframe = dataframe.with_columns(
        pl.col("codigo_emenda")
        .str.slice(0, 4)
        .cast(pl.Int32, strict=False)
        .alias("ano_emenda"),
        pl.col("ano_mes").cast(pl.Int32, strict=False),
        _money_expression("valor_recebido"),
    )

    dataframe = dataframe.with_columns(
        (
            pl.col("ano_mes")
            .cast(pl.String)
            .str.slice(0, 4)
            .cast(pl.Int32, strict=False)
        ).alias("ano_recebimento"),
        (
            pl.col("ano_mes")
            .cast(pl.String)
            .str.slice(4, 2)
            .cast(pl.Int8, strict=False)
        ).alias("mes_recebimento"),
    )

    return (
        dataframe.filter(pl.col("ano_emenda").is_in(years))
        .group_by(
            [
                "codigo_emenda",
                "codigo_autor_emenda",
                "nome_autor_emenda",
                "numero_emenda",
                "tipo_emenda",
                "ano_mes",
                "ano_emenda",
                "ano_recebimento",
                "mes_recebimento",
                "codigo_favorecido",
                "favorecido",
                "natureza_juridica",
                "tipo_favorecido",
                "uf_favorecido",
                "municipio_favorecido",
            ]
        )
        .agg(pl.col("valor_recebido").sum().alias("valor_recebido"))
        .sort(
            [
                "ano_emenda",
                "codigo_emenda",
                "valor_recebido",
            ],
            descending=[
                False,
                False,
                True,
            ],
        )
        .collect()
    )
