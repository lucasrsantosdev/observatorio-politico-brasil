from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


VALUE_COLUMNS = [
    "valorEmpenhado",
    "valorLiquidado",
    "valorPago",
    "valorRestoInscrito",
    "valorRestoCancelado",
    "valorRestoPago",
]


def _money_expression(column: str) -> pl.Expr:
    """
    Converte valores no padrão brasileiro:

    8.000,00 -> 8000.00
    """
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.replace_all(".", "", literal=True)
        .str.replace(",", ".", literal=True)
        .cast(pl.Decimal(18, 2), strict=False)
    )


def _read_bronze_pages(execution_path: Path) -> pl.DataFrame:
    page_files = sorted(execution_path.glob("pagina_*.json"))

    if not page_files:
        raise FileNotFoundError(
            f"Nenhuma página de emendas encontrada em {execution_path}"
        )

    records: list[dict[str, object]] = []

    for page_file in page_files:
        page_data = json.loads(
            page_file.read_text(encoding="utf-8")
        )

        if not isinstance(page_data, list):
            raise TypeError(
                f"O arquivo {page_file} não contém uma lista."
            )

        records.extend(page_data)

    if not records:
        raise ValueError(
            f"Nenhum registro foi encontrado em {execution_path}"
        )

    logger.info(
        "Páginas Bronze lidas: paginas=%s registros=%s",
        len(page_files),
        len(records),
    )

    return pl.DataFrame(records)


def transform_emendas(
    *,
    execution_path: Path,
) -> pl.DataFrame:
    dataframe = _read_bronze_pages(execution_path)

    required_columns = {
        "codigoEmenda",
        "ano",
        "tipoEmenda",
        "autor",
        "nomeAutor",
        "numeroEmenda",
        "localidadeDoGasto",
        "funcao",
        "subfuncao",
        *VALUE_COLUMNS,
    }

    missing_columns = required_columns.difference(
        dataframe.columns
    )

    if missing_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes: "
            f"{sorted(missing_columns)}"
        )

    dataframe = dataframe.with_columns(
        [
            _money_expression(column).alias(column)
            for column in VALUE_COLUMNS
        ]
    )

    dataframe = dataframe.with_columns(
        pl.col("localidadeDoGasto")
        .cast(pl.String)
        .str.extract(r"^(.*?)\s*-\s*([A-Z]{2})$", group_index=1)
        .str.strip_chars()
        .alias("municipio"),
        pl.col("localidadeDoGasto")
        .cast(pl.String)
        .str.extract(r"^(.*?)\s*-\s*([A-Z]{2})$", group_index=2)
        .str.strip_chars()
        .alias("uf"),
    )

    dataframe = dataframe.with_columns(
        pl.when(
            pl.col("valorEmpenhado").is_not_null()
            & (pl.col("valorEmpenhado") > 0)
        )
        .then(
            (
                pl.col("valorLiquidado").cast(pl.Float64)
                / pl.col("valorEmpenhado").cast(pl.Float64)
            )
            * 100
        )
        .otherwise(None)
        .round(4)
        .alias("percentual_liquidado"),
        pl.when(
            pl.col("valorEmpenhado").is_not_null()
            & (pl.col("valorEmpenhado") > 0)
        )
        .then(
            (
                pl.col("valorPago").cast(pl.Float64)
                / pl.col("valorEmpenhado").cast(pl.Float64)
            )
            * 100
        )
        .otherwise(None)
        .round(4)
        .alias("percentual_pago"),
    )

    dataframe = dataframe.rename(
        {
            "codigoEmenda": "codigo_emenda",
            "tipoEmenda": "tipo_emenda",
            "nomeAutor": "nome_autor",
            "numeroEmenda": "numero_emenda",
            "localidadeDoGasto": "localidade_gasto",
            "valorEmpenhado": "valor_empenhado",
            "valorLiquidado": "valor_liquidado",
            "valorPago": "valor_pago",
            "valorRestoInscrito": "valor_resto_inscrito",
            "valorRestoCancelado": "valor_resto_cancelado",
            "valorRestoPago": "valor_resto_pago",
        }
    )

    dataframe = (
        dataframe
        .unique(
            subset=["codigo_emenda"],
            keep="last",
        )
        .sort(
            [
                "ano",
                "nome_autor",
                "codigo_emenda",
            ]
        )
    )

    logger.info(
        "Transformação Silver concluída: registros=%s",
        dataframe.height,
    )

    return dataframe
