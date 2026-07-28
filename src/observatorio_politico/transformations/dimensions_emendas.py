from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def _remove_invalid_keys(
    dataframe: pl.DataFrame,
    column: str,
) -> pl.DataFrame:
    return dataframe.filter(
        pl.col(column).is_not_null()
        & (pl.col(column).cast(pl.String).str.strip_chars() != "")
    )


def _canonical_author(
    dataframe: pl.DataFrame,
) -> pl.DataFrame:
    source = (
        dataframe.select(
            "ano_emenda",
            "codigo_autor_emenda",
            "nome_autor_emenda",
        )
        .pipe(
            _remove_invalid_keys,
            "codigo_autor_emenda",
        )
        .with_columns(
            pl.col("codigo_autor_emenda").cast(pl.String).str.strip_chars(),
            pl.col("nome_autor_emenda").cast(pl.String).str.strip_chars(),
        )
    )

    names = (
        source.group_by(
            [
                "codigo_autor_emenda",
                "nome_autor_emenda",
            ]
        )
        .len()
        .sort(
            [
                "codigo_autor_emenda",
                "len",
                "nome_autor_emenda",
            ],
            descending=[
                False,
                True,
                False,
            ],
        )
        .unique(
            subset=["codigo_autor_emenda"],
            keep="first",
        )
        .drop("len")
    )

    metadata = source.group_by("codigo_autor_emenda").agg(
        pl.col("ano_emenda").min().alias("primeiro_ano_emenda"),
        pl.col("ano_emenda").max().alias("ultimo_ano_emenda"),
        pl.len().alias("quantidade_registros_origem"),
    )

    return names.join(
        metadata,
        on="codigo_autor_emenda",
        how="left",
    ).sort("nome_autor_emenda")


def _canonical_favorecido(
    dataframe: pl.DataFrame,
) -> pl.DataFrame:
    attributes = [
        "codigo_favorecido",
        "favorecido",
        "natureza_juridica",
        "tipo_favorecido",
        "uf_favorecido",
        "municipio_favorecido",
    ]

    source = (
        dataframe.select(
            "ano_emenda",
            *attributes,
        )
        .pipe(
            _remove_invalid_keys,
            "codigo_favorecido",
        )
        .with_columns(
            [pl.col(column).cast(pl.String).str.strip_chars() for column in attributes]
        )
    )

    canonical = (
        source.group_by(attributes)
        .len()
        .sort(
            [
                "codigo_favorecido",
                "len",
                "favorecido",
            ],
            descending=[
                False,
                True,
                False,
            ],
        )
        .unique(
            subset=["codigo_favorecido"],
            keep="first",
        )
        .drop("len")
    )

    metadata = source.group_by("codigo_favorecido").agg(
        pl.col("ano_emenda").min().alias("primeiro_ano_emenda"),
        pl.col("ano_emenda").max().alias("ultimo_ano_emenda"),
        pl.len().alias("quantidade_registros_origem"),
    )

    return canonical.join(
        metadata,
        on="codigo_favorecido",
        how="left",
    ).sort("favorecido")


def build_dim_ano(
    dataframes: list[pl.DataFrame],
) -> pl.DataFrame:
    current_year = datetime.now(UTC).year

    years = pl.concat(
        [
            dataframe.select(pl.col("ano_emenda").cast(pl.Int32))
            for dataframe in dataframes
            if "ano_emenda" in dataframe.columns
        ],
        how="vertical",
    )

    return (
        years.unique()
        .sort("ano_emenda")
        .with_columns(
            pl.when(pl.col("ano_emenda") >= current_year)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("dados_parciais"),
            pl.when(pl.col("ano_emenda") >= current_year)
            .then(pl.lit("Em andamento"))
            .otherwise(pl.lit("Encerrado"))
            .alias("status_exercicio"),
        )
    )


def build_dim_autor(
    ranking_parlamentares: pl.DataFrame,
    relacionamento_autor_favorecido: pl.DataFrame,
) -> pl.DataFrame:
    source = pl.concat(
        [
            ranking_parlamentares.select(
                "ano_emenda",
                "codigo_autor_emenda",
                "nome_autor_emenda",
            ),
            relacionamento_autor_favorecido.select(
                "ano_emenda",
                "codigo_autor_emenda",
                "nome_autor_emenda",
            ),
        ],
        how="vertical_relaxed",
    )

    return _canonical_author(source)


def build_dim_favorecido(
    ranking_favorecidos: pl.DataFrame,
    relacionamento_autor_favorecido: pl.DataFrame,
) -> pl.DataFrame:
    columns = [
        "ano_emenda",
        "codigo_favorecido",
        "favorecido",
        "natureza_juridica",
        "tipo_favorecido",
        "uf_favorecido",
        "municipio_favorecido",
    ]

    source = pl.concat(
        [
            ranking_favorecidos.select(columns),
            relacionamento_autor_favorecido.select(columns),
        ],
        how="vertical_relaxed",
    )

    return _canonical_favorecido(source)


def build_dim_funcao(
    ranking_funcoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        ranking_funcoes.select(
            "codigo_funcao",
            "nome_funcao",
        )
        .pipe(
            _remove_invalid_keys,
            "codigo_funcao",
        )
        .with_columns(
            pl.col("codigo_funcao").cast(pl.String).str.strip_chars(),
            pl.col("nome_funcao").cast(pl.String).str.strip_chars(),
        )
        .unique(
            subset=["codigo_funcao"],
            keep="first",
        )
        .sort("nome_funcao")
    )


def build_dim_uf(
    ranking_uf: pl.DataFrame,
    ranking_municipios: pl.DataFrame,
) -> pl.DataFrame:
    source = pl.concat(
        [
            ranking_uf.select(
                "uf",
                "regiao",
            ),
            ranking_municipios.select(
                "uf",
                "regiao",
            ),
        ],
        how="vertical_relaxed",
    )

    source = source.pipe(
        _remove_invalid_keys,
        "uf",
    ).with_columns(
        pl.col("uf").cast(pl.String).str.strip_chars().str.to_uppercase(),
        pl.col("regiao").cast(pl.String).str.strip_chars(),
    )

    canonical = (
        source.group_by("uf")
        .agg(pl.col("regiao").drop_nulls().mode().first().alias("regiao"))
        .with_columns(
            pl.when(pl.col("uf") == "MÚLTIPLO")
            .then(pl.lit("Múltiplas regiões"))
            .when(pl.col("uf") == "NACIONAL")
            .then(pl.lit("Nacional"))
            .otherwise(pl.col("regiao"))
            .alias("regiao")
        )
        .sort("uf")
    )

    return canonical


def build_dim_municipio(
    ranking_municipios: pl.DataFrame,
) -> pl.DataFrame:
    source = (
        ranking_municipios.select(
            "ano_emenda",
            "codigo_municipio_ibge",
            "municipio",
            "uf",
            "regiao",
        )
        .pipe(
            _remove_invalid_keys,
            "codigo_municipio_ibge",
        )
        .with_columns(
            pl.col("codigo_municipio_ibge").cast(pl.String).str.strip_chars(),
            pl.col("municipio").cast(pl.String).str.strip_chars(),
            pl.col("uf").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("regiao").cast(pl.String).str.strip_chars(),
        )
    )

    canonical = (
        source.group_by(
            [
                "codigo_municipio_ibge",
                "municipio",
                "uf",
                "regiao",
            ]
        )
        .len()
        .sort(
            [
                "codigo_municipio_ibge",
                "len",
                "municipio",
            ],
            descending=[
                False,
                True,
                False,
            ],
        )
        .unique(
            subset=["codigo_municipio_ibge"],
            keep="first",
        )
        .drop("len")
    )

    metadata = source.group_by("codigo_municipio_ibge").agg(
        pl.col("ano_emenda").min().alias("primeiro_ano_emenda"),
        pl.col("ano_emenda").max().alias("ultimo_ano_emenda"),
    )

    return canonical.join(
        metadata,
        on="codigo_municipio_ibge",
        how="left",
    ).sort(
        [
            "uf",
            "municipio",
        ]
    )
