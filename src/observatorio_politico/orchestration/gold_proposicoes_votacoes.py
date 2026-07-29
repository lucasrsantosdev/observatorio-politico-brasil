from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_proposicoes_votacoes import (
    build_fato_proposicoes,
    build_fato_votacoes,
    build_fato_votos,
    build_ranking_autores,
    build_ranking_deputados_votos,
    build_ranking_partidos_votos,
    build_ranking_temas,
    build_rel_proposicoes_autores,
    build_rel_proposicoes_temas,
    build_rel_votacoes_objetos,
    build_rel_votacoes_orientacoes,
    build_rel_votacoes_proposicoes,
    build_resumo_votacoes_mensal,
)

logger = logging.getLogger(__name__)


def _load(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Silver não encontrada: {path}")

    return pl.read_parquet(path)


def _write(
    *,
    dataframe: pl.DataFrame,
    root: Path,
    dataset: str,
) -> dict[str, object]:
    destination = root / dataset
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
        "Gold legislativa criada: dataset=%s registros=%s colunas=%s",
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


def run_gold_proposicoes_votacoes(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    years_label = "_".join(str(year) for year in normalized_years)

    silver_root = (
        Path("data/silver")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    silver_manifest_path = silver_root / "silver.manifest.json"

    if not silver_manifest_path.exists():
        raise FileNotFoundError(
            f"Manifesto Silver não encontrado: {silver_manifest_path}"
        )

    silver_manifest = json.loads(silver_manifest_path.read_text(encoding="utf-8"))

    if not silver_manifest.get(
        "approved",
        False,
    ):
        raise ValueError("Silver legislativa não está aprovada.")

    proposicoes = _load(
        silver_root,
        "proposicoes",
    )
    temas = _load(
        silver_root,
        "proposicoes_temas",
    )
    autores = _load(
        silver_root,
        "proposicoes_autores",
    )
    votacoes = _load(
        silver_root,
        "votacoes",
    )
    orientacoes = _load(
        silver_root,
        "votacoes_orientacoes",
    )
    votos = _load(
        silver_root,
        "votacoes_votos",
    )
    objetos = _load(
        silver_root,
        "votacoes_objetos",
    )
    relacoes = _load(
        silver_root,
        "votacoes_proposicoes",
    )

    fato_proposicoes = build_fato_proposicoes(proposicoes)

    rel_temas = build_rel_proposicoes_temas(temas)

    rel_autores = build_rel_proposicoes_autores(autores)

    fato_votacoes = build_fato_votacoes(votacoes)

    fato_votos = build_fato_votos(votos)

    datasets = {
        "fato_proposicoes": fato_proposicoes,
        "rel_proposicoes_temas": rel_temas,
        "rel_proposicoes_autores": rel_autores,
        "fato_votacoes": fato_votacoes,
        "fato_votos": fato_votos,
        "rel_votacoes_orientacoes": (build_rel_votacoes_orientacoes(orientacoes)),
        "rel_votacoes_objetos": (build_rel_votacoes_objetos(objetos)),
        "rel_votacoes_proposicoes": (build_rel_votacoes_proposicoes(relacoes)),
        "ranking_autores": (build_ranking_autores(rel_autores)),
        "ranking_temas": (build_ranking_temas(rel_temas)),
        "ranking_deputados_votos": (build_ranking_deputados_votos(fato_votos)),
        "ranking_partidos_votos": (build_ranking_partidos_votos(fato_votos)),
        "resumo_votacoes_mensal": (build_resumo_votacoes_mensal(fato_votacoes)),
    }

    gold_root = (
        Path("data/gold")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    outputs = [
        _write(
            dataframe=dataframe,
            root=gold_root,
            dataset=dataset,
        )
        for dataset, dataframe in datasets.items()
    ]

    manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "layer": "gold",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "silver_approved": True,
        "dataset_count": len(outputs),
        "datasets": outputs,
        "methodology_notes": [
            ("As tabelas fato preservam os registros detalhados da Silver."),
            (
                "Autores, temas, orientações, objetos e "
                "proposições afetadas permanecem em "
                "relacionamentos independentes."
            ),
            (
                "Objetos de votação não são tratados "
                "automaticamente como proposições afetadas."
            ),
            (
                "Rankings representam contagens analíticas "
                "e não avaliações de mérito parlamentar."
            ),
        ],
    }

    manifest_path = gold_root / "gold.manifest.json"

    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Gold legislativa concluída: datasets=%s manifesto=%s",
        len(outputs),
        manifest_path,
    )

    return manifest_path
