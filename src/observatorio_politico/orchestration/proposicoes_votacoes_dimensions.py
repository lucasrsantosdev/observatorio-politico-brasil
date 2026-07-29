from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _load(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset Gold nao encontrado: {path}")

    return pl.read_parquet(path)


def _existing_columns(
    dataframe: pl.DataFrame,
    columns: list[str],
) -> list[str]:
    return [column for column in columns if column in dataframe.columns]


def _write_dimension(
    *,
    dataframe: pl.DataFrame,
    root: Path,
    dimension: str,
) -> dict[str, object]:
    destination = root / dimension

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dimension}.parquet"

    csv_path = destination / f"{dimension}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    logger.info(
        "Dimensao legislativa criada: dimensao=%s registros=%s colunas=%s",
        dimension,
        dataframe.height,
        dataframe.width,
    )

    return {
        "dimension": dimension,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def _build_dim_proposicao(
    fato_proposicoes: pl.DataFrame,
) -> pl.DataFrame:
    columns = _existing_columns(
        fato_proposicoes,
        [
            "id_proposicao",
            "sigla_tipo",
            "cod_tipo",
            "numero",
            "ano",
            "titulo_proposicao",
            "ementa",
            "ementa_detalhada",
            "keywords",
            "data_apresentacao_data",
            "ano_apresentacao",
            "ultimo_status_descricao_situacao",
            "ultimo_status_sigla_orgao",
            "ultimo_status_descricao_tramitacao",
            "url_inteiro_teor",
            "possui_ementa",
            "possui_inteiro_teor",
        ],
    )

    return (
        fato_proposicoes.select(columns)
        .filter(pl.col("id_proposicao").is_not_null())
        .unique(
            subset=["id_proposicao"],
            keep="first",
        )
        .sort("id_proposicao")
    )


def _build_dim_autor(
    autores: pl.DataFrame,
) -> pl.DataFrame:
    columns = _existing_columns(
        autores,
        [
            "id_deputado_autor",
            "nome_autor",
            "tipo_autor",
            "categoria_autor",
            "cod_tipo_autor",
            "sigla_partido_autor",
            "sigla_uf_autor",
            "uri_autor",
        ],
    )

    dimension = autores.select(columns).unique()

    return dimension.with_row_index(
        name="id_autor_dim",
        offset=1,
    ).sort(
        [
            "categoria_autor",
            "nome_autor",
        ]
    )


def _build_dim_tema(
    temas: pl.DataFrame,
) -> pl.DataFrame:
    columns = _existing_columns(
        temas,
        [
            "cod_tema",
            "tema",
            "relevancia",
        ],
    )

    subset = [
        column
        for column in [
            "cod_tema",
            "tema",
        ]
        if column in columns
    ]

    return (
        temas.select(columns)
        .unique(
            subset=subset or None,
            keep="first",
        )
        .sort(
            [
                column
                for column in [
                    "cod_tema",
                    "tema",
                ]
                if column in columns
            ]
        )
    )


def _build_dim_deputado(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    columns = _existing_columns(
        votos,
        [
            "deputado_id",
            "deputado_nome",
            "deputado_sigla_partido",
            "deputado_sigla_uf",
            "deputado_id_legislatura",
            "deputado_uri",
            "deputado_url_foto",
        ],
    )

    return (
        votos.select(columns)
        .filter(pl.col("deputado_id").is_not_null())
        .unique(
            subset=["deputado_id"],
            keep="first",
        )
        .sort("deputado_nome")
    )


def _build_dim_partido(
    votos: pl.DataFrame,
    autores: pl.DataFrame,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    if "deputado_sigla_partido" in votos.columns:
        frames.append(
            votos.select(pl.col("deputado_sigla_partido").alias("sigla_partido"))
        )

    if "sigla_partido_autor" in autores.columns:
        frames.append(
            autores.select(pl.col("sigla_partido_autor").alias("sigla_partido"))
        )

    if not frames:
        return pl.DataFrame(
            schema={
                "id_partido": pl.Int64,
                "sigla_partido": pl.String,
            }
        )

    return (
        pl.concat(
            frames,
            how="vertical_relaxed",
        )
        .with_columns(
            pl.col("sigla_partido")
            .fill_null("SEM_PARTIDO")
            .str.strip_chars()
            .str.to_uppercase()
        )
        .unique()
        .sort("sigla_partido")
        .with_row_index(
            name="id_partido",
            offset=1,
        )
    )


def _build_dim_uf(
    votos: pl.DataFrame,
    autores: pl.DataFrame,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    if "deputado_sigla_uf" in votos.columns:
        frames.append(votos.select(pl.col("deputado_sigla_uf").alias("sigla_uf")))

    if "sigla_uf_autor" in autores.columns:
        frames.append(autores.select(pl.col("sigla_uf_autor").alias("sigla_uf")))

    return (
        pl.concat(
            frames,
            how="vertical_relaxed",
        )
        .filter(pl.col("sigla_uf").is_not_null())
        .with_columns(pl.col("sigla_uf").str.strip_chars().str.to_uppercase())
        .filter(pl.col("sigla_uf").str.len_chars() == 2)
        .unique()
        .sort("sigla_uf")
        .with_row_index(
            name="id_uf",
            offset=1,
        )
    )


def _build_dim_orgao(
    proposicoes: pl.DataFrame,
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    if "ultimo_status_id_orgao" in proposicoes.columns:
        expressions = [pl.col("ultimo_status_id_orgao").alias("id_orgao")]

        if "ultimo_status_sigla_orgao" in proposicoes.columns:
            expressions.append(pl.col("ultimo_status_sigla_orgao").alias("sigla_orgao"))

        frames.append(proposicoes.select(expressions))

    if "id_orgao" in votacoes.columns:
        expressions = [pl.col("id_orgao")]

        if "sigla_orgao" in votacoes.columns:
            expressions.append(pl.col("sigla_orgao"))

        frames.append(votacoes.select(expressions))

    return (
        pl.concat(
            frames,
            how="diagonal_relaxed",
        )
        .filter(pl.col("id_orgao").is_not_null())
        .unique(
            subset=["id_orgao"],
            keep="first",
        )
        .sort("id_orgao")
    )


def _build_dim_tipo_proposicao(
    proposicoes: pl.DataFrame,
) -> pl.DataFrame:
    columns = _existing_columns(
        proposicoes,
        [
            "cod_tipo",
            "sigla_tipo",
            "descricao_tipo",
        ],
    )

    subset = [
        column
        for column in [
            "cod_tipo",
            "sigla_tipo",
        ]
        if column in columns
    ]

    return (
        proposicoes.select(columns)
        .unique(
            subset=subset or None,
            keep="first",
        )
        .sort(
            [
                column
                for column in [
                    "cod_tipo",
                    "sigla_tipo",
                ]
                if column in columns
            ]
        )
    )


def _build_dim_tipo_voto(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.select(pl.col("categoria_voto").alias("tipo_voto"))
        .fill_null("NAO_INFORMADO")
        .unique()
        .sort("tipo_voto")
        .with_row_index(
            name="id_tipo_voto",
            offset=1,
        )
    )


def _build_dim_resultado_votacao(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votacoes.select("resultado_votacao")
        .fill_null("SEM_RESULTADO")
        .unique()
        .sort("resultado_votacao")
        .with_row_index(
            name="id_resultado_votacao",
            offset=1,
        )
    )


def _build_dim_tempo(
    proposicoes: pl.DataFrame,
    votacoes: pl.DataFrame,
    votos: pl.DataFrame,
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []

    mappings = [
        (
            proposicoes,
            "data_apresentacao_data",
        ),
        (
            votacoes,
            "data_votacao",
        ),
        (
            votos,
            "data_voto",
        ),
    ]

    for dataframe, column in mappings:
        if column in dataframe.columns:
            frames.append(dataframe.select(pl.col(column).alias("data")))

    dates = (
        pl.concat(
            frames,
            how="vertical_relaxed",
        )
        .filter(pl.col("data").is_not_null())
        .unique()
        .sort("data")
    )

    return dates.with_columns(
        pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("id_tempo"),
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().cast(pl.Int8).alias("mes"),
        pl.col("data").dt.day().cast(pl.Int8).alias("dia"),
        pl.col("data").dt.quarter().cast(pl.Int8).alias("trimestre"),
        pl.col("data").dt.weekday().cast(pl.Int8).alias("dia_semana_numero"),
        pl.col("data").dt.strftime("%Y-%m").alias("ano_mes"),
        pl.col("data").dt.strftime("%Y%m").alias("periodo"),
    ).select(
        "id_tempo",
        "data",
        "ano",
        "mes",
        "dia",
        "trimestre",
        "dia_semana_numero",
        "ano_mes",
        "periodo",
    )


def run_dimensions_proposicoes_votacoes(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    if not normalized_years:
        raise ValueError("Informe pelo menos um ano.")

    years_label = "_".join(str(year) for year in normalized_years)

    gold_root = (
        Path("data/gold")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    quality_path = gold_root / "quality.manifest.json"

    reconciliation_path = gold_root / "reconciliation.manifest.json"

    if not quality_path.exists():
        raise FileNotFoundError(f"Qualidade ausente: {quality_path}")

    if not reconciliation_path.exists():
        raise FileNotFoundError(f"Reconciliacao ausente: {reconciliation_path}")

    quality = json.loads(quality_path.read_text(encoding="utf-8"))

    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))

    if not quality.get("approved", False):
        raise ValueError("Qualidade legislativa nao aprovada.")

    if not reconciliation.get(
        "approved",
        False,
    ):
        raise ValueError("Reconciliacao legislativa nao aprovada.")

    proposicoes = _load(
        gold_root,
        "fato_proposicoes",
    )

    autores = _load(
        gold_root,
        "rel_proposicoes_autores",
    )

    temas = _load(
        gold_root,
        "rel_proposicoes_temas",
    )

    votacoes = _load(
        gold_root,
        "fato_votacoes",
    )

    votos = _load(
        gold_root,
        "fato_votos",
    )

    dimensions = {
        "dim_proposicao": (_build_dim_proposicao(proposicoes)),
        "dim_autor": (_build_dim_autor(autores)),
        "dim_tema": (_build_dim_tema(temas)),
        "dim_deputado": (_build_dim_deputado(votos)),
        "dim_partido": (
            _build_dim_partido(
                votos,
                autores,
            )
        ),
        "dim_uf": (
            _build_dim_uf(
                votos,
                autores,
            )
        ),
        "dim_orgao": (
            _build_dim_orgao(
                proposicoes,
                votacoes,
            )
        ),
        "dim_tipo_proposicao": (_build_dim_tipo_proposicao(proposicoes)),
        "dim_tipo_voto": (_build_dim_tipo_voto(votos)),
        "dim_resultado_votacao": (_build_dim_resultado_votacao(votacoes)),
        "dim_tempo": (
            _build_dim_tempo(
                proposicoes,
                votacoes,
                votos,
            )
        ),
    }

    dimensions_root = gold_root / "dimensions"

    outputs = [
        _write_dimension(
            dataframe=dataframe,
            root=dimensions_root,
            dimension=dimension,
        )
        for dimension, dataframe in dimensions.items()
    ]

    manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "layer": "gold",
        "artifact_type": "dimensions",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "quality_approved": True,
        "reconciliation_approved": True,
        "dimension_count": len(outputs),
        "approved": all(int(output["record_count"]) > 0 for output in outputs),
        "dimensions": outputs,
    }

    manifest_path = dimensions_root / "dimensions.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("DIMENSOES LEGISLATIVAS")
    print("=" * 100)

    for output in outputs:
        print(
            f"{output['dimension']:<35} "
            f"registros={output['record_count']:<10} "
            f"colunas={output['column_count']}"
        )

    print()
    print(f"DIMENSION_COUNT={len(outputs)}")

    print(f"DIMENSIONS_APPROVED={manifest['approved']}")

    return manifest_path
