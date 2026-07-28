from __future__ import annotations

import polars as pl


def build_concorrencia_licitacoes(
    licitacoes: pl.DataFrame,
    participantes: pl.DataFrame,
    itens: pl.DataFrame,
) -> pl.DataFrame:
    concorrencia = participantes.group_by("chave_licitacao").agg(
        pl.col("codigo_participante")
        .drop_nulls()
        .n_unique()
        .alias("quantidade_participantes"),
        pl.col("codigo_participante")
        .filter(pl.col("participante_vencedor"))
        .drop_nulls()
        .n_unique()
        .alias("quantidade_vencedores"),
        pl.col("codigo_item_compra")
        .drop_nulls()
        .n_unique()
        .alias("quantidade_itens_disputados"),
        pl.col("participante_sigiloso")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_participacoes_sigilosas"),
    )

    itens_resumo = itens.group_by("chave_licitacao").agg(
        pl.col("codigo_item_compra")
        .drop_nulls()
        .n_unique()
        .alias("quantidade_itens_resultado"),
        pl.col("codigo_vencedor")
        .filter(pl.col("vencedor_identificado"))
        .drop_nulls()
        .n_unique()
        .alias("quantidade_fornecedores_vencedores"),
        pl.col("valor_item").sum().alias("valor_itens_vencedores"),
    )

    resultado = (
        licitacoes.join(
            concorrencia,
            on="chave_licitacao",
            how="left",
        )
        .join(
            itens_resumo,
            on="chave_licitacao",
            how="left",
        )
        .with_columns(
            pl.col("quantidade_participantes").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_vencedores").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_itens_disputados").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_participacoes_sigilosas").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_itens_resultado").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_fornecedores_vencedores").fill_null(0).cast(pl.Int32),
            pl.col("valor_itens_vencedores").fill_null(0.0),
        )
        .with_columns(
            (pl.col("quantidade_participantes") <= 1).alias("baixa_competitividade"),
            (pl.col("quantidade_participantes") == 1).alias("participante_unico"),
            (pl.col("quantidade_participantes") == 0).alias(
                "sem_participante_identificado"
            ),
            (pl.col("quantidade_vencedores") == 0).alias("sem_vencedor_identificado"),
        )
    )

    return resultado.sort(
        [
            "data_resultado_compra",
            "valor_licitacao",
        ],
        descending=[
            True,
            True,
        ],
        nulls_last=True,
    )


def build_ranking_orgaos(
    licitacoes: pl.DataFrame,
    concorrencia: pl.DataFrame,
) -> pl.DataFrame:
    concorrencia_resumo = concorrencia.group_by(
        [
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
        ]
    ).agg(
        pl.col("quantidade_participantes").mean().round(4).alias("media_participantes"),
        pl.col("quantidade_participantes").median().alias("mediana_participantes"),
        pl.col("baixa_competitividade")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_baixa_competitividade"),
        pl.col("participante_unico")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_participante_unico"),
        pl.col("licitacao_sigilosa")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_licitacoes_sigilosas"),
    )

    ranking = (
        licitacoes.group_by(
            [
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
            ]
        )
        .agg(
            pl.col("chave_licitacao").n_unique().alias("quantidade_licitacoes"),
            pl.col("codigo_ug").drop_nulls().n_unique().alias("quantidade_ugs"),
            pl.col("codigo_modalidade_compra")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_modalidades"),
            pl.col("valor_licitacao").sum().alias("valor_total_licitado"),
            pl.col("valor_licitacao").mean().round(2).alias("valor_medio_licitacao"),
            pl.col("valor_licitacao").max().alias("maior_valor_licitacao"),
        )
        .join(
            concorrencia_resumo,
            on=[
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
            ],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("quantidade_licitacoes") > 0)
            .then(
                pl.col("quantidade_baixa_competitividade")
                / pl.col("quantidade_licitacoes")
                * 100
            )
            .otherwise(None)
            .round(4)
            .alias("percentual_baixa_competitividade")
        )
        .with_columns(
            pl.col("valor_total_licitado")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_licitado")
        )
    )

    return ranking.sort(
        "valor_total_licitado",
        descending=True,
    )


def build_ranking_fornecedores(
    itens: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        itens.filter(pl.col("vencedor_identificado"))
        .group_by(
            [
                "codigo_vencedor",
                "nome_vencedor",
            ]
        )
        .agg(
            pl.col("chave_licitacao")
            .n_unique()
            .alias("quantidade_licitacoes_vencidas"),
            pl.col("chave_item_licitacao")
            .n_unique()
            .alias("quantidade_itens_vencidos"),
            pl.col("codigo_orgao").drop_nulls().n_unique().alias("quantidade_orgaos"),
            pl.col("valor_item").sum().alias("valor_itens_vencidos"),
            pl.col("valor_item").mean().round(2).alias("valor_medio_item"),
            pl.col("valor_item").max().alias("maior_valor_item"),
        )
        .with_columns(
            pl.col("valor_itens_vencidos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_itens_vencidos"),
            pl.col("quantidade_licitacoes_vencidas")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_quantidade_licitacoes"),
        )
    )

    return ranking.sort(
        "valor_itens_vencidos",
        descending=True,
    )


def build_ranking_modalidades(
    licitacoes: pl.DataFrame,
    concorrencia: pl.DataFrame,
) -> pl.DataFrame:
    concorrencia_modalidade = concorrencia.group_by(
        [
            "codigo_modalidade_compra",
            "modalidade_compra",
        ]
    ).agg(
        pl.col("quantidade_participantes").mean().round(4).alias("media_participantes"),
        pl.col("baixa_competitividade")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_baixa_competitividade"),
        pl.col("participante_unico")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_participante_unico"),
    )

    ranking = (
        licitacoes.group_by(
            [
                "codigo_modalidade_compra",
                "modalidade_compra",
            ]
        )
        .agg(
            pl.col("chave_licitacao").n_unique().alias("quantidade_licitacoes"),
            pl.col("codigo_orgao").drop_nulls().n_unique().alias("quantidade_orgaos"),
            pl.col("valor_licitacao").sum().alias("valor_total_licitado"),
            pl.col("valor_licitacao").mean().round(2).alias("valor_medio_licitacao"),
            pl.col("licitacao_sigilosa")
            .sum()
            .cast(pl.Int32)
            .alias("quantidade_sigilosas"),
        )
        .join(
            concorrencia_modalidade,
            on=[
                "codigo_modalidade_compra",
                "modalidade_compra",
            ],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("quantidade_licitacoes") > 0)
            .then(
                pl.col("quantidade_baixa_competitividade")
                / pl.col("quantidade_licitacoes")
                * 100
            )
            .otherwise(None)
            .round(4)
            .alias("percentual_baixa_competitividade")
        )
        .with_columns(
            pl.col("valor_total_licitado")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_licitado")
        )
    )

    return ranking.sort(
        "valor_total_licitado",
        descending=True,
    )


def build_ranking_uf(
    licitacoes: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        licitacoes.filter(pl.col("uf").is_not_null())
        .group_by(
            [
                "uf",
                "municipio",
            ]
        )
        .agg(
            pl.col("chave_licitacao").n_unique().alias("quantidade_licitacoes"),
            pl.col("codigo_orgao").drop_nulls().n_unique().alias("quantidade_orgaos"),
            pl.col("valor_licitacao").sum().alias("valor_total_licitado"),
            pl.col("valor_licitacao").mean().round(2).alias("valor_medio_licitacao"),
        )
        .with_columns(
            pl.col("valor_total_licitado")
            .rank(
                method="dense",
                descending=True,
            )
            .over("uf")
            .cast(pl.Int32)
            .alias("posicao_municipio_na_uf")
        )
    )

    return ranking.sort(
        [
            "uf",
            "valor_total_licitado",
        ],
        descending=[
            False,
            True,
        ],
    )


def build_relacionamento_orgao_fornecedor(
    licitacoes: pl.DataFrame,
    itens: pl.DataFrame,
) -> pl.DataFrame:
    metadata_orgao = licitacoes.select(
        "chave_licitacao",
        "codigo_orgao_superior",
        "nome_orgao_superior",
        "codigo_orgao",
        "nome_orgao",
        "codigo_ug",
        "nome_ug",
        "uf",
        "municipio",
    )

    relacionamento = (
        itens.filter(pl.col("vencedor_identificado"))
        .join(
            metadata_orgao,
            on="chave_licitacao",
            how="left",
            suffix="_licitacao",
        )
        .group_by(
            [
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
                "codigo_vencedor",
                "nome_vencedor",
            ]
        )
        .agg(
            pl.col("chave_licitacao").n_unique().alias("quantidade_licitacoes"),
            pl.col("chave_item_licitacao").n_unique().alias("quantidade_itens"),
            pl.col("valor_item").sum().alias("valor_itens_vencidos"),
        )
        .with_columns(
            pl.col("valor_itens_vencidos")
            .sum()
            .over("codigo_orgao")
            .alias("total_itens_vencidos_orgao")
        )
        .with_columns(
            pl.when(pl.col("total_itens_vencidos_orgao") > 0)
            .then(
                pl.col("valor_itens_vencidos")
                / pl.col("total_itens_vencidos_orgao")
                * 100
            )
            .otherwise(None)
            .round(4)
            .alias("percentual_concentracao_fornecedor"),
            pl.col("valor_itens_vencidos")
            .rank(
                method="dense",
                descending=True,
            )
            .over("codigo_orgao")
            .cast(pl.Int32)
            .alias("posicao_fornecedor_no_orgao"),
        )
    )

    return relacionamento.sort(
        [
            "nome_orgao",
            "valor_itens_vencidos",
        ],
        descending=[
            False,
            True,
        ],
    )
