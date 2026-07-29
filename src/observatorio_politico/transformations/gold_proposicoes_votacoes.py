from __future__ import annotations

import polars as pl


def _normalize_vote() -> pl.Expr:
    return (
        pl.col("voto")
        .cast(pl.String)
        .str.strip_chars()
        .str.to_lowercase()
        .str.replace_all("ã", "a")
        .str.replace_all("ç", "c")
        .str.replace_all("é", "e")
        .str.replace_all("í", "i")
        .str.replace_all("ó", "o")
        .str.replace_all("ú", "u")
        .alias("voto_normalizado")
    )


def build_fato_proposicoes(
    proposicoes: pl.DataFrame,
) -> pl.DataFrame:
    return proposicoes.with_columns(
        pl.col("id").cast(pl.Int64).alias("id_proposicao"),
        pl.concat_str(
            [
                pl.col("sigla_tipo").fill_null(""),
                pl.col("numero").cast(pl.String).fill_null(""),
                pl.col("ano").cast(pl.String).fill_null(""),
            ],
            separator=" ",
        ).alias("titulo_proposicao"),
        pl.col("data_apresentacao").dt.date().alias("data_apresentacao_data"),
        pl.col("data_apresentacao").dt.year().alias("ano_apresentacao"),
        pl.col("data_apresentacao").dt.month().cast(pl.Int8).alias("mes_apresentacao"),
        pl.col("data_apresentacao").dt.strftime("%Y%m").alias("periodo_apresentacao"),
        pl.col("ultimo_status_data_hora").dt.date().alias("data_ultimo_status"),
        pl.col("ementa").is_not_null().alias("possui_ementa"),
        pl.col("url_inteiro_teor").is_not_null().alias("possui_inteiro_teor"),
    ).sort(
        [
            "ano",
            "sigla_tipo",
            "numero",
            "id_proposicao",
        ]
    )


def build_rel_proposicoes_temas(
    temas: pl.DataFrame,
) -> pl.DataFrame:
    return temas.with_columns(
        pl.col("uri_proposicao")
        .str.extract(r"/(\d+)$", 1)
        .cast(pl.Int64, strict=False)
        .alias("id_proposicao"),
        pl.concat_str(
            [
                pl.col("uri_proposicao").fill_null(""),
                pl.col("cod_tema").cast(pl.String).fill_null(""),
                pl.col("tema").fill_null(""),
            ],
            separator="|",
        ).alias("chave_proposicao_tema"),
    ).sort(
        [
            "id_proposicao",
            "cod_tema",
        ]
    )


def build_rel_proposicoes_autores(
    autores: pl.DataFrame,
) -> pl.DataFrame:
    rename_map: dict[str, str] = {}

    if "sigla_ufautor" in autores.columns and "sigla_uf_autor" not in autores.columns:
        rename_map["sigla_ufautor"] = "sigla_uf_autor"

    if rename_map:
        autores = autores.rename(rename_map)

    return autores.with_columns(
        pl.col("id_proposicao").cast(pl.Int64),
        pl.col("id_deputado_autor").cast(pl.Int64, strict=False),
        pl.concat_str(
            [
                pl.col("id_proposicao").cast(pl.String),
                pl.col("id_deputado_autor").cast(pl.String).fill_null("SEM_DEPUTADO"),
                pl.col("cod_tipo_autor").cast(pl.String).fill_null(""),
                pl.col("ordem_assinatura").cast(pl.String).fill_null(""),
            ],
            separator="|",
        ).alias("chave_proposicao_autor"),
        pl.when(pl.col("id_deputado_autor").is_not_null())
        .then(pl.lit("DEPUTADO"))
        .otherwise(pl.col("tipo_autor").fill_null("OUTRO").str.to_uppercase())
        .alias("categoria_autor"),
    ).sort(
        [
            "id_proposicao",
            "ordem_assinatura",
            "nome_autor",
        ]
    )


def build_fato_votacoes(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return votacoes.with_columns(
        pl.col("id").alias("id_votacao"),
        pl.col("data_hora_registro").dt.date().alias("data_votacao"),
        pl.col("data_hora_registro").dt.year().alias("ano_votacao"),
        pl.col("data_hora_registro").dt.month().cast(pl.Int8).alias("mes_votacao"),
        pl.col("data_hora_registro").dt.strftime("%Y%m").alias("periodo_votacao"),
        (
            pl.col("votos_sim").fill_null(0)
            + pl.col("votos_nao").fill_null(0)
            + pl.col("votos_outros").fill_null(0)
        ).alias("total_votos_informados"),
        pl.when(pl.col("aprovacao") == True)
        .then(pl.lit("APROVADA"))
        .when(pl.col("aprovacao") == False)
        .then(pl.lit("REJEITADA"))
        .otherwise(pl.lit("SEM_RESULTADO"))
        .alias("resultado_votacao"),
    ).sort(
        [
            "data_hora_registro",
            "id_votacao",
        ]
    )


def build_fato_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.with_columns(
            _normalize_vote(),
            pl.col("data_hora_voto").dt.date().alias("data_voto"),
            pl.col("data_hora_voto").dt.year().alias("ano_voto"),
            pl.col("data_hora_voto").dt.month().cast(pl.Int8).alias("mes_voto"),
            pl.col("data_hora_voto").dt.strftime("%Y%m").alias("periodo_voto"),
            pl.concat_str(
                [
                    pl.col("id_votacao"),
                    pl.col("deputado_id").cast(pl.String).fill_null("SEM_DEPUTADO"),
                    pl.col("data_hora_voto").cast(pl.String).fill_null(""),
                    pl.col("linha_arquivo").cast(pl.String),
                ],
                separator="|",
            ).alias("chave_voto"),
        )
        .with_columns(
            pl.when(pl.col("voto_normalizado") == "sim")
            .then(pl.lit("SIM"))
            .when(pl.col("voto_normalizado").is_in(["nao"]))
            .then(pl.lit("NAO"))
            .when(pl.col("voto_normalizado").is_in(["abstencao"]))
            .then(pl.lit("ABSTENCAO"))
            .when(pl.col("voto_normalizado").is_in(["obstrucao"]))
            .then(pl.lit("OBSTRUCAO"))
            .otherwise(pl.col("voto").fill_null("NAO_INFORMADO").str.to_uppercase())
            .alias("categoria_voto")
        )
        .sort(
            [
                "data_hora_voto",
                "id_votacao",
                "deputado_id",
            ]
        )
    )


def build_rel_votacoes_orientacoes(
    orientacoes: pl.DataFrame,
) -> pl.DataFrame:
    return orientacoes.with_columns(
        pl.concat_str(
            [
                pl.col("id_votacao"),
                pl.col("sigla_bancada").fill_null("SEM_BANCADA"),
                pl.col("orientacao").fill_null("SEM_ORIENTACAO"),
                pl.col("linha_arquivo").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_votacao_orientacao")
    ).sort(
        [
            "id_votacao",
            "sigla_bancada",
        ]
    )


def build_rel_votacoes_objetos(
    objetos: pl.DataFrame,
) -> pl.DataFrame:
    return objetos.with_columns(
        pl.concat_str(
            [
                pl.col("id_votacao"),
                pl.col("proposicao_id").cast(pl.String).fill_null("SEM_PROPOSICAO"),
                pl.col("linha_arquivo").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_votacao_objeto")
    ).sort(
        [
            "id_votacao",
            "proposicao_id",
        ]
    )


def build_rel_votacoes_proposicoes(
    relacoes: pl.DataFrame,
) -> pl.DataFrame:
    return relacoes.with_columns(
        pl.concat_str(
            [
                pl.col("id_votacao"),
                pl.col("proposicao_id").cast(pl.String).fill_null("SEM_PROPOSICAO"),
                pl.col("linha_arquivo").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_votacao_proposicao")
    ).sort(
        [
            "id_votacao",
            "proposicao_id",
        ]
    )


def build_ranking_autores(
    autores: pl.DataFrame,
) -> pl.DataFrame:
    return (
        autores.group_by(
            [
                "id_deputado_autor",
                "nome_autor",
                "categoria_autor",
                "sigla_partido_autor",
                "sigla_uf_autor",
            ]
        )
        .agg(
            pl.col("id_proposicao").n_unique().alias("quantidade_proposicoes"),
            pl.len().alias("quantidade_autorias"),
            pl.col("proponente")
            .fill_null(False)
            .sum()
            .alias("quantidade_como_proponente"),
        )
        .with_columns(
            pl.col("quantidade_proposicoes")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            [
                "quantidade_proposicoes",
                "nome_autor",
            ],
            descending=[True, False],
        )
    )


def build_ranking_temas(
    temas: pl.DataFrame,
) -> pl.DataFrame:
    return (
        temas.group_by(
            [
                "cod_tema",
                "tema",
            ]
        )
        .agg(
            pl.col("id_proposicao").n_unique().alias("quantidade_proposicoes"),
            pl.len().alias("quantidade_classificacoes"),
            pl.col("relevancia").mean().round(2).alias("relevancia_media"),
        )
        .with_columns(
            pl.col("quantidade_proposicoes")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "quantidade_proposicoes",
            descending=True,
        )
    )


def build_ranking_deputados_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.group_by(
            [
                "deputado_id",
                "deputado_nome",
                "deputado_sigla_partido",
                "deputado_sigla_uf",
            ]
        )
        .agg(
            pl.len().alias("quantidade_votos"),
            pl.col("id_votacao").n_unique().alias("quantidade_votacoes"),
            (pl.col("categoria_voto") == "SIM").sum().alias("votos_sim"),
            (pl.col("categoria_voto") == "NAO").sum().alias("votos_nao"),
            (pl.col("categoria_voto") == "ABSTENCAO").sum().alias("abstencoes"),
            (pl.col("categoria_voto") == "OBSTRUCAO").sum().alias("obstrucoes"),
        )
        .with_columns(
            pl.col("quantidade_votos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "quantidade_votos",
            descending=True,
        )
    )


def build_ranking_partidos_votos(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.with_columns(pl.col("deputado_sigla_partido").fill_null("SEM_PARTIDO"))
        .group_by("deputado_sigla_partido")
        .agg(
            pl.col("deputado_id").n_unique().alias("quantidade_deputados"),
            pl.len().alias("quantidade_votos"),
            pl.col("id_votacao").n_unique().alias("quantidade_votacoes"),
            (pl.col("categoria_voto") == "SIM").sum().alias("votos_sim"),
            (pl.col("categoria_voto") == "NAO").sum().alias("votos_nao"),
            (pl.col("categoria_voto") == "ABSTENCAO").sum().alias("abstencoes"),
            (pl.col("categoria_voto") == "OBSTRUCAO").sum().alias("obstrucoes"),
        )
        .with_columns(
            pl.col("quantidade_votos")
            .rank(
                method="dense",
                descending=True,
            )
            .cast(pl.Int32)
            .alias("posicao")
        )
        .sort(
            "quantidade_votos",
            descending=True,
        )
    )


def build_resumo_votacoes_mensal(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votacoes.group_by(
            [
                "ano_votacao",
                "mes_votacao",
                "periodo_votacao",
            ]
        )
        .agg(
            pl.len().alias("quantidade_votacoes"),
            (pl.col("resultado_votacao") == "APROVADA")
            .sum()
            .alias("quantidade_aprovadas"),
            (pl.col("resultado_votacao") == "REJEITADA")
            .sum()
            .alias("quantidade_rejeitadas"),
            (pl.col("resultado_votacao") == "SEM_RESULTADO")
            .sum()
            .alias("quantidade_sem_resultado"),
            pl.col("votos_sim").fill_null(0).sum().alias("total_votos_sim"),
            pl.col("votos_nao").fill_null(0).sum().alias("total_votos_nao"),
            pl.col("votos_outros").fill_null(0).sum().alias("total_votos_outros"),
        )
        .sort(
            [
                "ano_votacao",
                "mes_votacao",
            ]
        )
    )
