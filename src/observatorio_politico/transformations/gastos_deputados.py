from __future__ import annotations

import polars as pl

TEXT_COLUMNS = [
    "txNomeParlamentar",
    "cpf",
    "ideCadastro",
    "nuCarteiraParlamentar",
    "sgUF",
    "sgPartido",
    "txtDescricao",
    "txtDescricaoEspecificacao",
    "txtFornecedor",
    "txtCNPJCPF",
    "txtNumero",
    "datEmissao",
    "txtPassageiro",
    "txtTrecho",
    "numRessarcimento",
    "datPagamentoRestituicao",
    "vlrRestituicao",
    "urlDocumento",
]


def _normalize_text(
    column: str,
) -> pl.Expr:
    return (
        pl.col(column).cast(pl.String).str.strip_chars().replace("", None).alias(column)
    )


def _parse_datetime(
    column: str,
) -> pl.Expr:
    return pl.col(column).str.to_datetime(
        "%Y-%m-%dT%H:%M:%S",
        strict=False,
    )


def transform_gastos_deputados(
    raw: pl.DataFrame,
) -> pl.DataFrame:
    normalized = raw.with_columns(
        [_normalize_text(column) for column in TEXT_COLUMNS if column in raw.columns]
    )

    normalized = normalized.with_columns(
        _parse_datetime("datEmissao").alias("data_hora_emissao"),
        _parse_datetime("datPagamentoRestituicao").alias(
            "data_hora_pagamento_restituicao"
        ),
        pl.col("vlrRestituicao")
        .str.replace_all(",", ".")
        .cast(pl.Float64, strict=False)
        .alias("valor_restituicao"),
    )

    normalized = normalized.with_columns(
        pl.col("data_hora_emissao").dt.date().alias("data_emissao"),
        pl.col("data_hora_pagamento_restituicao")
        .dt.date()
        .alias("data_pagamento_restituicao"),
        pl.col("nuDeputadoId").cast(pl.Int64).alias("codigo_beneficiario"),
        pl.col("txNomeParlamentar").alias("nome_beneficiario"),
        pl.col("sgUF").alias("uf"),
        pl.col("sgPartido").alias("partido"),
        pl.col("nuLegislatura").cast(pl.Int32).alias("numero_legislatura"),
        pl.col("codLegislatura").cast(pl.Int32).alias("codigo_legislatura"),
        pl.col("numSubCota").cast(pl.Int32).alias("codigo_tipo_despesa"),
        pl.col("txtDescricao").alias("tipo_despesa"),
        pl.col("numEspecificacaoSubCota")
        .cast(pl.Int32)
        .alias("codigo_especificacao_despesa"),
        pl.col("txtDescricaoEspecificacao").alias("especificacao_despesa"),
        pl.col("txtFornecedor").alias("nome_fornecedor"),
        pl.col("txtCNPJCPF").alias("documento_fornecedor"),
        pl.col("txtNumero").alias("numero_documento"),
        pl.col("indTipoDocumento").cast(pl.Int32).alias("codigo_tipo_documento"),
        pl.col("vlrDocumento").cast(pl.Float64).alias("valor_documento"),
        pl.col("vlrGlosa").cast(pl.Float64).alias("valor_glosa"),
        pl.col("vlrLiquido").cast(pl.Float64).alias("valor_liquido"),
        pl.col("numMes").cast(pl.Int8).alias("mes_despesa"),
        pl.col("numAno").cast(pl.Int32).alias("ano_despesa"),
        pl.col("numParcela").cast(pl.Int32).alias("numero_parcela"),
        pl.col("txtPassageiro").alias("passageiro"),
        pl.col("txtTrecho").alias("trecho"),
        pl.col("numLote").cast(pl.Int64).alias("numero_lote"),
        pl.col("numRessarcimento").alias("numero_ressarcimento"),
        pl.col("ideDocumento").cast(pl.Int64).alias("codigo_documento"),
        pl.col("urlDocumento").alias("url_documento"),
    )

    normalized = normalized.with_columns(
        pl.when(
            pl.col("cpf").is_not_null()
            | pl.col("ideCadastro").is_not_null()
            | pl.col("nuCarteiraParlamentar").is_not_null()
        )
        .then(pl.lit("PARLAMENTAR"))
        .otherwise(pl.lit("LIDERANCA_OU_ORGAO"))
        .alias("tipo_beneficiario"),
        pl.when(
            (pl.col("valor_liquido") < 0)
            & pl.col("codigo_tipo_despesa").is_in([998, 999])
        )
        .then(pl.lit("ESTORNO_AEREO"))
        .when(pl.col("valor_liquido") < 0)
        .then(pl.lit("ESTORNO_OU_AJUSTE"))
        .otherwise(pl.lit("DESPESA"))
        .alias("tipo_movimento"),
        (pl.col("valor_documento") - pl.col("valor_glosa") - pl.col("valor_liquido"))
        .round(2)
        .alias("diferenca_financeira"),
        pl.col("valor_restituicao").is_not_null().alias("possui_restituicao"),
        pl.col("data_emissao").is_null().alias("data_emissao_ausente"),
        pl.col("documento_fornecedor").is_null().alias("fornecedor_sem_documento"),
    )

    normalized = normalized.with_columns(
        (pl.col("diferenca_financeira").abs() <= 0.01).alias("financeiro_consistente"),
        pl.concat_str(
            [
                pl.col("ano_arquivo_origem").cast(pl.String),
                pl.col("arquivo_origem"),
                pl.col("linha_arquivo").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_despesa"),
        pl.when(pl.col("codigo_documento") > 0)
        .then(
            pl.concat_str(
                [
                    pl.col("ano_arquivo_origem").cast(pl.String),
                    pl.col("codigo_documento").cast(pl.String),
                ],
                separator="|",
            )
        )
        .otherwise(
            pl.concat_str(
                [
                    pl.lit("SEM_DOCUMENTO"),
                    pl.col("ano_arquivo_origem").cast(pl.String),
                    pl.col("codigo_beneficiario").cast(pl.String),
                    pl.col("linha_arquivo").cast(pl.String),
                ],
                separator="|",
            )
        )
        .alias("chave_documento"),
        pl.concat_str(
            [
                pl.col("codigo_beneficiario").cast(pl.String),
                pl.col("nome_beneficiario").fill_null(""),
            ],
            separator="|",
        ).alias("chave_beneficiario"),
        pl.concat_str(
            [
                pl.col("documento_fornecedor").fill_null("SEM_DOCUMENTO"),
                pl.col("nome_fornecedor").fill_null("NÃO INFORMADO"),
            ],
            separator="|",
        ).alias("chave_fornecedor"),
        pl.concat_str(
            [
                pl.col("ano_despesa").cast(pl.String),
                pl.col("mes_despesa").cast(pl.String).str.pad_start(2, "0"),
            ],
        ).alias("periodo_despesa"),
    )

    return normalized.select(
        "chave_despesa",
        "chave_documento",
        "chave_beneficiario",
        "chave_fornecedor",
        "codigo_documento",
        "codigo_beneficiario",
        "nome_beneficiario",
        "tipo_beneficiario",
        "cpf",
        "ideCadastro",
        "nuCarteiraParlamentar",
        "uf",
        "partido",
        "numero_legislatura",
        "codigo_legislatura",
        "codigo_tipo_despesa",
        "tipo_despesa",
        "codigo_especificacao_despesa",
        "especificacao_despesa",
        "nome_fornecedor",
        "documento_fornecedor",
        "numero_documento",
        "codigo_tipo_documento",
        "data_hora_emissao",
        "data_emissao",
        "valor_documento",
        "valor_glosa",
        "valor_liquido",
        "diferenca_financeira",
        "financeiro_consistente",
        "valor_restituicao",
        "possui_restituicao",
        "data_hora_pagamento_restituicao",
        "data_pagamento_restituicao",
        "ano_despesa",
        "mes_despesa",
        "periodo_despesa",
        "numero_parcela",
        "passageiro",
        "trecho",
        "numero_lote",
        "numero_ressarcimento",
        "tipo_movimento",
        "data_emissao_ausente",
        "fornecedor_sem_documento",
        "url_documento",
        "ano_arquivo_origem",
        "arquivo_origem",
        "linha_arquivo",
    )
