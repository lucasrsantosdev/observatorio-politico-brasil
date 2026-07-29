from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


GOLD_ROOT = Path("data/gold/senado_federal/anos=2025_2026")

DIM_ROOT = Path("data/gold/senado_federal/anos=2025_2026/dimensoes")


def _read(dataset: str) -> pl.DataFrame:
    path = GOLD_ROOT / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset Gold nao encontrado: {path}")

    return pl.read_parquet(path)


def _write(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
) -> dict[str, Any]:
    destination = DIM_ROOT / dataset

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dataset}.parquet"

    csv_path = destination / f"{dataset}.csv"

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
        "Dimensao Senado criada: dataset=%s registros=%s colunas=%s",
        dataset,
        dataframe.height,
        dataframe.width,
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def _build_dim_senador(
    senadores_base: pl.DataFrame,
    votos: pl.DataFrame,
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    atuais = senadores_base.select(
        [
            pl.col("identificacao_parlamentar_codigo_parlamentar").alias(
                "codigo_senador"
            ),
            pl.col("identificacao_parlamentar_nome_parlamentar").alias("nome_senador"),
            pl.col("identificacao_parlamentar_nome_completo_parlamentar").alias(
                "nome_completo"
            ),
            pl.col("identificacao_parlamentar_sigla_partido_parlamentar").alias(
                "sigla_partido"
            ),
            pl.col("identificacao_parlamentar_uf_parlamentar").alias("sigla_uf"),
            pl.col("identificacao_parlamentar_sexo_parlamentar").alias("sexo"),
            pl.lit(True).alias("senador_em_exercicio"),
        ]
    ).filter(pl.col("codigo_senador").is_not_null())

    encontrados_votos = (
        votos.select(
            [
                pl.col("codigo_parlamentar").alias("codigo_senador"),
                pl.col("nome_parlamentar").alias("nome_senador"),
                pl.col("sigla_partido_parlamentar").alias("sigla_partido"),
                pl.col("sigla_uf_parlamentar").alias("sigla_uf"),
                pl.col("sexo_parlamentar").alias("sexo"),
            ]
        )
        .unique(
            subset=["codigo_senador"],
            keep="first",
        )
        .with_columns(
            pl.lit(None).cast(pl.String).alias("nome_completo"),
            pl.lit(False).alias("senador_em_exercicio"),
        )
    )

    encontrados_gastos = (
        gastos.select(
            [
                pl.col("cod_senador").alias("codigo_senador"),
                pl.col("nome_senador"),
            ]
        )
        .unique(
            subset=["codigo_senador"],
            keep="first",
        )
        .with_columns(
            pl.lit(None).cast(pl.String).alias("nome_completo"),
            pl.lit(None).cast(pl.String).alias("sigla_partido"),
            pl.lit(None).cast(pl.String).alias("sigla_uf"),
            pl.lit(None).cast(pl.String).alias("sexo"),
            pl.lit(False).alias("senador_em_exercicio"),
        )
    )

    return (
        pl.concat(
            [
                atuais,
                encontrados_votos.select(atuais.columns),
                encontrados_gastos.select(atuais.columns),
            ],
            how="vertical_relaxed",
        )
        .sort(
            "senador_em_exercicio",
            descending=True,
        )
        .unique(
            subset=["codigo_senador"],
            keep="first",
        )
        .with_columns(
            pl.col("codigo_senador").cast(pl.Int64).alias("id_senador"),
            pl.col("nome_senador").fill_null("NAO INFORMADO"),
            pl.col("sigla_partido").fill_null("SEM PARTIDO"),
            pl.col("sigla_uf").fill_null("NI"),
        )
        .select(
            [
                "id_senador",
                "codigo_senador",
                "nome_senador",
                "nome_completo",
                "sigla_partido",
                "sigla_uf",
                "sexo",
                "senador_em_exercicio",
            ]
        )
        .sort("nome_senador")
    )


def _build_dim_materia(
    materias: pl.DataFrame,
) -> pl.DataFrame:
    return (
        materias.select(
            [
                "id_materia",
                "codigo",
                "identificacao_processo",
                "descricao_identificacao",
                "sigla",
                "numero",
                "ano",
                "sigla_comissao",
                "ementa",
                "autor",
                "data",
                "url_detalhe_materia",
                "titulo_materia",
            ]
        )
        .unique(
            subset=["id_materia"],
            keep="first",
        )
        .sort("id_materia")
    )


def _build_dim_partido(
    votos: pl.DataFrame,
    senadores: pl.DataFrame,
) -> pl.DataFrame:
    from_votos = votos.select(
        pl.col("sigla_partido_parlamentar").alias("sigla_partido")
    )

    from_senadores = senadores.select("sigla_partido")

    return (
        pl.concat(
            [
                from_votos,
                from_senadores,
            ],
            how="vertical_relaxed",
        )
        .with_columns(
            pl.col("sigla_partido")
            .fill_null("SEM PARTIDO")
            .str.strip_chars()
            .str.to_uppercase()
        )
        .unique()
        .sort("sigla_partido")
        .with_row_index(
            "id_partido",
            offset=1,
        )
    )


def _build_dim_uf(
    votos: pl.DataFrame,
    senadores: pl.DataFrame,
) -> pl.DataFrame:
    from_votos = votos.select(pl.col("sigla_uf_parlamentar").alias("sigla_uf"))

    from_senadores = senadores.select("sigla_uf")

    return (
        pl.concat(
            [
                from_votos,
                from_senadores,
            ],
            how="vertical_relaxed",
        )
        .with_columns(
            pl.col("sigla_uf").fill_null("NI").str.strip_chars().str.to_uppercase()
        )
        .unique()
        .sort("sigla_uf")
        .with_row_index(
            "id_uf",
            offset=1,
        )
    )


def _build_dim_tipo_materia(
    materias: pl.DataFrame,
) -> pl.DataFrame:
    return (
        materias.select(
            [
                pl.col("sigla").alias("sigla_tipo_materia"),
                pl.col("descricao_identificacao").alias("descricao_tipo_materia"),
            ]
        )
        .with_columns(pl.col("sigla_tipo_materia").fill_null("NI").str.to_uppercase())
        .unique(
            subset=["sigla_tipo_materia"],
            keep="first",
        )
        .sort("sigla_tipo_materia")
        .with_row_index(
            "id_tipo_materia",
            offset=1,
        )
    )


def _build_dim_tipo_voto(
    votos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votos.select(
            [
                pl.col("categoria_voto"),
                pl.col("sigla_voto_parlamentar").alias("descricao_voto_original"),
            ]
        )
        .unique()
        .sort(
            [
                "categoria_voto",
                "descricao_voto_original",
            ]
        )
        .with_row_index(
            "id_tipo_voto",
            offset=1,
        )
    )


def _build_dim_resultado_votacao(
    votacoes: pl.DataFrame,
) -> pl.DataFrame:
    return (
        votacoes.select(
            [
                pl.col("resultado_votacao").alias("resultado_original"),
                pl.col("resultado_votacao_normalizado").alias("resultado_normalizado"),
            ]
        )
        .unique()
        .sort("resultado_normalizado")
        .with_row_index(
            "id_resultado_votacao",
            offset=1,
        )
    )


def _build_dim_tipo_despesa(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.select("tipo_despesa")
        .with_columns(
            pl.col("tipo_despesa").fill_null("NAO INFORMADO").str.strip_chars()
        )
        .unique()
        .sort("tipo_despesa")
        .with_row_index(
            "id_tipo_despesa",
            offset=1,
        )
    )


def _build_dim_fornecedor(
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    return (
        gastos.select(
            [
                pl.col("cpf_cnpj_fornecedor_normalizado").alias("documento_fornecedor"),
                pl.col("cpf_cnpj_fornecedor").alias("documento_fornecedor_formatado"),
                "nome_fornecedor",
            ]
        )
        .unique(
            subset=[
                "documento_fornecedor",
                "nome_fornecedor",
            ],
            keep="first",
        )
        .with_columns(
            pl.when(pl.col("documento_fornecedor").str.len_chars() == 14)
            .then(pl.lit("CNPJ"))
            .when(pl.col("documento_fornecedor").str.len_chars() == 11)
            .then(pl.lit("CPF"))
            .otherwise(pl.lit("OUTRO"))
            .alias("tipo_documento")
        )
        .with_row_index(
            "id_fornecedor",
            offset=1,
        )
        .sort("nome_fornecedor")
    )


def _build_dim_tempo(
    materias: pl.DataFrame,
    votacoes: pl.DataFrame,
    gastos: pl.DataFrame,
) -> pl.DataFrame:
    minimum = date(2025, 1, 1)
    maximum = date(2026, 12, 31)

    logger.info(
        "Criando dim_tempo com recorte fixo: minimum=%s maximum=%s",
        minimum,
        maximum,
    )

    rows: list[dict[str, Any]] = []

    current = minimum

    while current <= maximum:
        rows.append(
            {
                "data": current,
                "ano": current.year,
                "mes": current.month,
                "dia": current.day,
                "ano_mes": (f"{current.year}{current.month:02d}"),
                "nome_mes": current.strftime("%B"),
                "trimestre": (f"T{((current.month - 1) // 3) + 1}"),
                "dia_semana": current.isoweekday(),
                "nome_dia_semana": (current.strftime("%A")),
            }
        )

        current += timedelta(days=1)

    dataframe = (
        pl.DataFrame(rows)
        .with_columns(
            pl.col("data").dt.strftime("%Y%m%d").cast(pl.Int32).alias("id_tempo")
        )
        .select(
            [
                "id_tempo",
                "data",
                "ano",
                "mes",
                "dia",
                "ano_mes",
                "nome_mes",
                "trimestre",
                "dia_semana",
                "nome_dia_semana",
            ]
        )
    )

    if dataframe.height != 730:
        raise RuntimeError(
            "Dimensao tempo deveria possuir "
            f"730 registros, mas possui "
            f"{dataframe.height}."
        )

    return dataframe


def run_senado_dimensions() -> Path:
    quality_path = Path("output/auditoria/senado/senado_quality.json")

    quality = json.loads(quality_path.read_text(encoding="utf-8"))

    if not quality.get("approved"):
        raise RuntimeError("Qualidade do Senado nao aprovada.")

    materias = _read("fato_materias")

    votacoes = _read("fato_votacoes")

    votos = _read("fato_votos")

    gastos = _read("fato_gastos_senadores")

    senadores_base = _read("dim_senadores_base")

    dim_senador = _build_dim_senador(
        senadores_base,
        votos,
        gastos,
    )

    datasets = {
        "dim_senador": dim_senador,
        "dim_materia": _build_dim_materia(materias),
        "dim_partido": _build_dim_partido(
            votos,
            dim_senador,
        ),
        "dim_uf": _build_dim_uf(
            votos,
            dim_senador,
        ),
        "dim_tipo_materia": (_build_dim_tipo_materia(materias)),
        "dim_tipo_voto": (_build_dim_tipo_voto(votos)),
        "dim_resultado_votacao": (_build_dim_resultado_votacao(votacoes)),
        "dim_tipo_despesa": (_build_dim_tipo_despesa(gastos)),
        "dim_fornecedor": (_build_dim_fornecedor(gastos)),
        "dim_tempo": _build_dim_tempo(
            materias,
            votacoes,
            gastos,
        ),
    }

    outputs = [
        _write(
            dataframe,
            dataset=dataset,
        )
        for dataset, dataframe in datasets.items()
    ]

    approved = all(int(item["record_count"]) > 0 for item in outputs)

    manifest = {
        "source": "senado_federal",
        "layer": "dimensions",
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "quality_approved": True,
        "dimension_count": len(outputs),
        "approved": approved,
        "dimensions": outputs,
    }

    manifest_path = DIM_ROOT / "dimensions.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("DIMENSOES DO SENADO FEDERAL")
    print("=" * 100)

    for item in outputs:
        print(
            f"{item['dataset']:<35} "
            f"registros={item['record_count']:<8} "
            f"colunas={item['column_count']}"
        )

    print()
    print(f"DIMENSION_COUNT={len(outputs)}")
    print(f"DIMENSIONS_APPROVED={approved}")
    print(f"MANIFESTO={manifest_path}")

    return manifest_path
