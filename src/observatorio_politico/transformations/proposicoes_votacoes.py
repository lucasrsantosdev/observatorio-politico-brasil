from __future__ import annotations

import hashlib

import polars as pl

INTEGER_COLUMNS: dict[str, set[str]] = {
    "proposicoes": {
        "id",
        "numero",
        "ano",
        "codTipo",
        "ultimoStatus_sequencia",
        "ultimoStatus_idOrgao",
        "ultimoStatus_idTipoTramitacao",
        "ultimoStatus_idSituacao",
    },
    "proposicoes_temas": {
        "numero",
        "ano",
        "codTema",
        "relevancia",
    },
    "proposicoes_autores": {
        "idProposicao",
        "idDeputadoAutor",
        "codTipoAutor",
        "ordemAssinatura",
    },
    "votacoes": {
        "idOrgao",
        "idEvento",
        "votosSim",
        "votosNao",
        "votosOutros",
        "ultimaApresentacaoProposicao_idProposicao",
    },
    "votacoes_orientacoes": set(),
    "votacoes_votos": {
        "deputado_id",
        "deputado_idLegislatura",
    },
    "votacoes_objetos": {
        "proposicao_id",
        "proposicao_codTipo",
        "proposicao_numero",
        "proposicao_ano",
    },
    "votacoes_proposicoes": {
        "proposicao_id",
        "proposicao_codTipo",
        "proposicao_numero",
        "proposicao_ano",
    },
}


DATETIME_COLUMNS: dict[str, set[str]] = {
    "proposicoes": {
        "dataApresentacao",
        "ultimoStatus_dataHora",
    },
    "proposicoes_temas": set(),
    "proposicoes_autores": set(),
    "votacoes": {
        "dataHoraRegistro",
        "ultimaAberturaVotacao_dataHoraRegistro",
        "ultimaApresentacaoProposicao_dataHoraRegistro",
    },
    "votacoes_orientacoes": set(),
    "votacoes_votos": {
        "dataHoraVoto",
    },
    "votacoes_objetos": set(),
    "votacoes_proposicoes": set(),
}


DATE_COLUMNS: dict[str, set[str]] = {
    "proposicoes": set(),
    "proposicoes_temas": set(),
    "proposicoes_autores": set(),
    "votacoes": {
        "data",
    },
    "votacoes_orientacoes": set(),
    "votacoes_votos": set(),
    "votacoes_objetos": {
        "data",
    },
    "votacoes_proposicoes": {
        "data",
    },
}


BOOLEAN_COLUMNS: dict[str, set[str]] = {
    "proposicoes": set(),
    "proposicoes_temas": set(),
    "proposicoes_autores": {
        "proponente",
    },
    "votacoes": {
        "aprovacao",
    },
    "votacoes_orientacoes": set(),
    "votacoes_votos": set(),
    "votacoes_objetos": set(),
    "votacoes_proposicoes": set(),
}


def _normalize_name(name: str) -> str:
    result: list[str] = []

    for index, character in enumerate(name):
        if character.isupper() and index > 0:
            previous = name[index - 1]

            if previous.islower() or previous.isdigit():
                result.append("_")

        result.append(character.lower())

    return "".join(result)


def _normalize_boolean(column: str) -> pl.Expr:
    normalized = pl.col(column).cast(pl.String).str.strip_chars().str.to_lowercase()

    return (
        pl.when(
            normalized.is_in(
                [
                    "true",
                    "sim",
                    "s",
                    "1",
                    "yes",
                ]
            )
        )
        .then(pl.lit(True))
        .when(
            normalized.is_in(
                [
                    "false",
                    "não",
                    "nao",
                    "n",
                    "0",
                    "no",
                ]
            )
        )
        .then(pl.lit(False))
        .otherwise(None)
        .alias(column)
    )


def _build_row_hash(
    columns: list[str],
) -> pl.Expr:
    expressions = [
        pl.col(column).cast(pl.String).fill_null("")
        for column in columns
        if column
        not in {
            "linha_arquivo",
            "arquivo_origem",
            "ano_arquivo_origem",
        }
    ]

    return (
        pl.concat_str(
            expressions,
            separator="|",
        )
        .map_elements(
            lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest(),
            return_dtype=pl.String,
        )
        .alias("hash_registro")
    )


def transform_dataset(
    raw: pl.DataFrame,
    *,
    dataset: str,
) -> pl.DataFrame:
    if dataset not in INTEGER_COLUMNS:
        raise ValueError(f"Dataset não configurado: {dataset}")

    original_columns = [
        column
        for column in raw.columns
        if column
        not in {
            "linha_arquivo",
            "arquivo_origem",
            "ano_arquivo_origem",
        }
    ]

    transformed = raw.with_columns(
        [
            pl.col(column)
            .cast(pl.String)
            .str.strip_chars()
            .replace("", None)
            .alias(column)
            for column in original_columns
        ]
    )

    integer_columns = INTEGER_COLUMNS[dataset] & set(transformed.columns)

    datetime_columns = DATETIME_COLUMNS[dataset] & set(transformed.columns)

    date_columns = DATE_COLUMNS[dataset] & set(transformed.columns)

    boolean_columns = BOOLEAN_COLUMNS[dataset] & set(transformed.columns)

    expressions: list[pl.Expr] = []

    expressions.extend(
        pl.col(column).cast(pl.Int64, strict=False).alias(column)
        for column in integer_columns
    )

    expressions.extend(
        pl.col(column)
        .str.to_datetime(
            strict=False,
        )
        .alias(column)
        for column in datetime_columns
    )

    expressions.extend(
        pl.col(column)
        .str.to_date(
            strict=False,
        )
        .alias(column)
        for column in date_columns
    )

    expressions.extend(_normalize_boolean(column) for column in boolean_columns)

    if expressions:
        transformed = transformed.with_columns(expressions)

    rename_map = {
        column: _normalize_name(column)
        for column in transformed.columns
        if column
        not in {
            "linha_arquivo",
            "arquivo_origem",
            "ano_arquivo_origem",
        }
    }

    transformed = transformed.rename(rename_map)

    normalized_data_columns = [
        rename_map.get(column, column) for column in original_columns
    ]

    transformed = transformed.with_columns(
        pl.concat_str(
            [
                pl.lit(dataset),
                pl.col("ano_arquivo_origem").cast(pl.String),
                pl.col("arquivo_origem"),
                pl.col("linha_arquivo").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_registro"),
        pl.lit(dataset).alias("dataset"),
        _build_row_hash(
            [
                rename_map.get(
                    column,
                    column,
                )
                for column in original_columns
            ]
        ),
    )

    return transformed.select(
        "chave_registro",
        "hash_registro",
        "dataset",
        *normalized_data_columns,
        "ano_arquivo_origem",
        "arquivo_origem",
        "linha_arquivo",
    )
