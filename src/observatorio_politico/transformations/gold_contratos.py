from __future__ import annotations

import polars as pl


def build_itens_contrato_atuais(
    itens: pl.DataFrame,
) -> pl.DataFrame:
    itens_preparados = itens.with_columns(
        pl.concat_str(
            [
                pl.col("chave_grupo_contrato"),
                pl.col("codigo_item_compra").fill_null(""),
            ],
            separator="|",
        ).alias("chave_logica_item")
    )

    itens_identificados = itens_preparados.filter(~pl.col("item_codigo_zero"))

    itens_codigo_zero = itens_preparados.filter(pl.col("item_codigo_zero"))

    periodo_mais_recente = itens_identificados.group_by("chave_logica_item").agg(
        pl.col("periodo_origem").max().alias("periodo_mais_recente_item")
    )

    itens_identificados_atuais = (
        itens_identificados.join(
            periodo_mais_recente,
            on="chave_logica_item",
            how="inner",
        )
        .filter(pl.col("periodo_origem") == pl.col("periodo_mais_recente_item"))
        .with_columns(
            pl.lit("codigo_item").alias("criterio_selecao_item"),
            pl.lit(True).alias("item_utilizavel_valor_oficial"),
        )
    )

    itens_codigo_zero = itens_codigo_zero.with_columns(
        pl.col("periodo_origem").alias("periodo_mais_recente_item"),
        pl.lit("registro_fisico_codigo_zero").alias("criterio_selecao_item"),
        pl.lit(False).alias("item_utilizavel_valor_oficial"),
    )

    return pl.concat(
        [
            itens_identificados_atuais,
            itens_codigo_zero,
        ],
        how="vertical_relaxed",
        rechunk=True,
    ).sort(
        [
            "chave_grupo_contrato",
            "codigo_item_compra",
            "periodo_origem",
        ]
    )


def build_contratos_atuais_resumo(
    contratos_atuais: pl.DataFrame,
    itens_atuais: pl.DataFrame,
    termos: pl.DataFrame,
) -> pl.DataFrame:
    contratos_grupo = (
        contratos_atuais.group_by("chave_grupo_contrato")
        .agg(
            pl.len().cast(pl.Int32).alias("quantidade_registros_atuais"),
            pl.col("chave_registro_contrato")
            .first()
            .alias("chave_registro_representativo"),
            pl.col("numero_contrato").first().alias("numero_contrato"),
            pl.col("codigo_orgao_superior")
            .drop_nulls()
            .first()
            .alias("codigo_orgao_superior"),
            pl.col("nome_orgao_superior")
            .drop_nulls()
            .first()
            .alias("nome_orgao_superior"),
            pl.col("codigo_orgao").drop_nulls().first().alias("codigo_orgao"),
            pl.col("nome_orgao").drop_nulls().first().alias("nome_orgao"),
            pl.col("codigo_ug").drop_nulls().first().alias("codigo_ug"),
            pl.col("nome_ug").drop_nulls().first().alias("nome_ug"),
            pl.col("codigo_contratado")
            .drop_nulls()
            .n_unique()
            .cast(pl.Int32)
            .alias("quantidade_contratados_grupo"),
            pl.col("codigo_contratado")
            .drop_nulls()
            .first()
            .alias("codigo_contratado_representativo"),
            pl.col("nome_contratado")
            .drop_nulls()
            .first()
            .alias("nome_contratado_representativo"),
            pl.col("objeto").drop_nulls().first().alias("objeto_representativo"),
            pl.col("modalidade_compra").drop_nulls().first().alias("modalidade_compra"),
            pl.col("situacao_contrato").drop_nulls().first().alias("situacao_contrato"),
            pl.col("data_assinatura_contrato").min().alias("primeira_data_assinatura"),
            pl.col("data_assinatura_contrato").max().alias("ultima_data_assinatura"),
            pl.col("data_inicio_vigencia").min().alias("primeiro_inicio_vigencia"),
            pl.col("data_fim_vigencia").max().alias("ultimo_fim_vigencia"),
            pl.col("valor_inicial_compra").sum().alias("valor_inicial_total_grupo"),
            pl.col("valor_final_compra").sum().alias("valor_final_total_grupo"),
            pl.col("variacao_valor_contrato").sum().alias("variacao_total_grupo"),
            pl.col("contrato_sigiloso").any().alias("possui_contrato_sigiloso"),
            pl.col("periodo_origem").max().alias("periodo_origem"),
        )
        .with_columns(
            (pl.col("quantidade_registros_atuais") > 1).alias(
                "grupo_com_multiplos_registros"
            ),
            pl.when(pl.col("valor_inicial_total_grupo") > 0)
            .then(
                pl.col("variacao_total_grupo")
                / pl.col("valor_inicial_total_grupo")
                * 100
            )
            .otherwise(None)
            .round(4)
            .alias("percentual_variacao_grupo"),
        )
    )

    itens_resumo = (
        itens_atuais.group_by("chave_grupo_contrato")
        .agg(
            pl.col("chave_item_contrato").n_unique().alias("quantidade_itens"),
            pl.col("chave_item_contrato")
            .filter(pl.col("item_utilizavel_valor_oficial"))
            .n_unique()
            .alias("quantidade_itens_identificados"),
            pl.col("chave_item_contrato")
            .filter(pl.col("item_codigo_zero"))
            .n_unique()
            .alias("quantidade_itens_codigo_zero"),
            pl.col("valor_total_item_calculado")
            .filter(pl.col("item_utilizavel_valor_oficial"))
            .sum()
            .alias("valor_total_itens_identificados"),
            pl.col("valor_total_item_calculado")
            .filter(pl.col("item_codigo_zero"))
            .sum()
            .alias("valor_total_itens_codigo_zero"),
            pl.col("item_sigiloso")
            .sum()
            .cast(pl.Int32)
            .alias("quantidade_itens_sigilosos"),
        )
        .with_columns(
            (
                pl.col("valor_total_itens_identificados")
                + pl.col("valor_total_itens_codigo_zero")
            ).alias("valor_total_itens_informativo")
        )
    )

    termos_resumo = termos.group_by("chave_grupo_contrato").agg(
        pl.col("chave_termo_aditivo").n_unique().alias("quantidade_termos_aditivos"),
        pl.col("termo_prorrogacao")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_prorrogacoes"),
        pl.col("termo_reajuste").sum().cast(pl.Int32).alias("quantidade_reajustes"),
        pl.col("termo_alteracao_quantitativa")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_alteracoes_quantitativas"),
    )

    return (
        contratos_grupo.join(
            itens_resumo,
            on="chave_grupo_contrato",
            how="left",
        )
        .join(
            termos_resumo,
            on="chave_grupo_contrato",
            how="left",
        )
        .with_columns(
            pl.col("quantidade_itens").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_itens_identificados").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_itens_codigo_zero").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_itens_sigilosos").fill_null(0).cast(pl.Int32),
            pl.col("valor_total_itens_identificados").fill_null(0.0),
            pl.col("valor_total_itens_codigo_zero").fill_null(0.0),
            pl.col("valor_total_itens_informativo").fill_null(0.0),
            pl.col("quantidade_termos_aditivos").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_prorrogacoes").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_reajustes").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_alteracoes_quantitativas").fill_null(0).cast(pl.Int32),
        )
        .with_columns(
            (
                pl.col("valor_final_total_grupo") > pl.col("valor_inicial_total_grupo")
            ).alias("grupo_com_acrescimo"),
            (
                pl.col("valor_final_total_grupo") < pl.col("valor_inicial_total_grupo")
            ).alias("grupo_com_reducao"),
            (pl.col("quantidade_termos_aditivos") > 0).alias("possui_termo_aditivo"),
            (pl.col("quantidade_itens_sigilosos") > 0).alias("possui_item_sigiloso"),
        )
        .sort(
            "valor_final_total_grupo",
            descending=True,
            nulls_last=True,
        )
    )


def build_ranking_contratados(
    contratos_atuais: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        contratos_atuais.filter(pl.col("codigo_contratado").is_not_null())
        .group_by(
            [
                "codigo_contratado",
                "nome_contratado",
            ]
        )
        .agg(
            pl.col("chave_registro_contrato").n_unique().alias("quantidade_contratos"),
            pl.col("chave_grupo_contrato")
            .n_unique()
            .alias("quantidade_grupos_contrato"),
            pl.col("codigo_orgao").drop_nulls().n_unique().alias("quantidade_orgaos"),
            pl.col("codigo_ug").drop_nulls().n_unique().alias("quantidade_ugs"),
            pl.col("valor_inicial_compra").sum().alias("valor_inicial_total"),
            pl.col("valor_final_compra").sum().alias("valor_final_total"),
            pl.col("variacao_valor_contrato").sum().alias("variacao_total"),
            pl.col("valor_final_compra").mean().round(2).alias("valor_medio_contrato"),
            pl.col("valor_final_compra").max().alias("maior_valor_contrato"),
            pl.col("contrato_sigiloso")
            .sum()
            .cast(pl.Int32)
            .alias("quantidade_contratos_sigilosos"),
        )
        .with_columns(
            pl.col("valor_final_total")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_final"),
            pl.col("quantidade_contratos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_quantidade_contratos"),
        )
    )

    return ranking.sort(
        "valor_final_total",
        descending=True,
    )


def build_ranking_orgaos(
    contratos_atuais: pl.DataFrame,
) -> pl.DataFrame:
    ranking = (
        contratos_atuais.group_by(
            [
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
            ]
        )
        .agg(
            pl.col("chave_registro_contrato").n_unique().alias("quantidade_contratos"),
            pl.col("codigo_contratado")
            .drop_nulls()
            .n_unique()
            .alias("quantidade_contratados"),
            pl.col("codigo_ug").drop_nulls().n_unique().alias("quantidade_ugs"),
            pl.col("valor_inicial_compra").sum().alias("valor_inicial_total"),
            pl.col("valor_final_compra").sum().alias("valor_final_total"),
            pl.col("variacao_valor_contrato").sum().alias("variacao_total"),
            pl.col("valor_final_compra").mean().round(2).alias("valor_medio_contrato"),
            pl.col("valor_final_compra").max().alias("maior_valor_contrato"),
            pl.col("contrato_sigiloso")
            .sum()
            .cast(pl.Int32)
            .alias("quantidade_contratos_sigilosos"),
        )
        .with_columns(
            pl.col("valor_final_total")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao_valor_final")
        )
    )

    return ranking.sort(
        "valor_final_total",
        descending=True,
    )


def build_contratos_com_variacao(
    contratos_atuais: pl.DataFrame,
) -> pl.DataFrame:
    return (
        contratos_atuais.filter(
            pl.col("variacao_valor_contrato").is_not_null()
            & (pl.col("variacao_valor_contrato") != 0)
        )
        .select(
            "chave_registro_contrato",
            "chave_grupo_contrato",
            "numero_contrato",
            "codigo_orgao_superior",
            "nome_orgao_superior",
            "codigo_orgao",
            "nome_orgao",
            "codigo_ug",
            "nome_ug",
            "codigo_contratado",
            "nome_contratado",
            "objeto",
            "data_assinatura_contrato",
            "data_inicio_vigencia",
            "data_fim_vigencia",
            "valor_inicial_compra",
            "valor_final_compra",
            "variacao_valor_contrato",
            "percentual_variacao_valor",
            "periodo_origem",
        )
        .sort(
            "variacao_valor_contrato",
            descending=True,
        )
    )


def build_termos_por_contrato(
    contratos_atuais: pl.DataFrame,
    termos: pl.DataFrame,
) -> pl.DataFrame:
    contratos_grupo = contratos_atuais.group_by("chave_grupo_contrato").agg(
        pl.len().cast(pl.Int32).alias("quantidade_registros_atuais"),
        pl.col("numero_contrato").first().alias("numero_contrato"),
        pl.col("codigo_orgao").drop_nulls().first().alias("codigo_orgao"),
        pl.col("nome_orgao").drop_nulls().first().alias("nome_orgao"),
        pl.col("codigo_ug").drop_nulls().first().alias("codigo_ug"),
        pl.col("nome_ug").drop_nulls().first().alias("nome_ug"),
        pl.col("codigo_contratado")
        .drop_nulls()
        .first()
        .alias("codigo_contratado_representativo"),
        pl.col("nome_contratado")
        .drop_nulls()
        .first()
        .alias("nome_contratado_representativo"),
        pl.col("valor_inicial_compra").sum().alias("valor_inicial_total_grupo"),
        pl.col("valor_final_compra").sum().alias("valor_final_total_grupo"),
        pl.col("variacao_valor_contrato").sum().alias("variacao_total_grupo"),
    )

    termos_resumo = termos.group_by("chave_grupo_contrato").agg(
        pl.col("chave_termo_aditivo").n_unique().alias("quantidade_termos"),
        pl.col("data_publicacao").min().alias("primeira_publicacao_termo"),
        pl.col("data_publicacao").max().alias("ultima_publicacao_termo"),
        pl.col("termo_prorrogacao")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_prorrogacoes"),
        pl.col("termo_reajuste").sum().cast(pl.Int32).alias("quantidade_reajustes"),
        pl.col("termo_alteracao_quantitativa")
        .sum()
        .cast(pl.Int32)
        .alias("quantidade_alteracoes_quantitativas"),
    )

    return (
        contratos_grupo.join(
            termos_resumo,
            on="chave_grupo_contrato",
            how="left",
        )
        .with_columns(
            pl.col("quantidade_termos").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_prorrogacoes").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_reajustes").fill_null(0).cast(pl.Int32),
            pl.col("quantidade_alteracoes_quantitativas").fill_null(0).cast(pl.Int32),
        )
        .sort(
            "quantidade_termos",
            descending=True,
        )
    )


def build_relacionamento_orgao_contratado(
    contratos_atuais: pl.DataFrame,
) -> pl.DataFrame:
    relacionamento = (
        contratos_atuais.filter(pl.col("codigo_contratado").is_not_null())
        .group_by(
            [
                "codigo_orgao_superior",
                "nome_orgao_superior",
                "codigo_orgao",
                "nome_orgao",
                "codigo_contratado",
                "nome_contratado",
            ]
        )
        .agg(
            pl.col("chave_registro_contrato").n_unique().alias("quantidade_contratos"),
            pl.col("valor_final_compra").sum().alias("valor_final_total"),
            pl.col("valor_inicial_compra").sum().alias("valor_inicial_total"),
            pl.col("variacao_valor_contrato").sum().alias("variacao_total"),
        )
        .with_columns(
            pl.col("valor_final_total")
            .sum()
            .over("codigo_orgao")
            .alias("valor_total_orgao")
        )
        .with_columns(
            pl.when(pl.col("valor_total_orgao") > 0)
            .then(pl.col("valor_final_total") / pl.col("valor_total_orgao") * 100)
            .otherwise(None)
            .round(4)
            .alias("percentual_concentracao"),
            pl.col("valor_final_total")
            .rank(
                method="dense",
                descending=True,
            )
            .over("codigo_orgao")
            .cast(pl.Int32)
            .alias("posicao_contratado_no_orgao"),
        )
    )

    return relacionamento.sort(
        [
            "nome_orgao",
            "valor_final_total",
        ],
        descending=[
            False,
            True,
        ],
    )
