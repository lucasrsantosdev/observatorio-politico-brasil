from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.gold_licitacoes import (
    build_concorrencia_licitacoes,
    build_ranking_fornecedores,
    build_ranking_modalidades,
    build_ranking_orgaos,
    build_ranking_uf,
    build_relacionamento_orgao_fornecedor,
)

logger = logging.getLogger(__name__)


def _read_silver(
    root: Path,
    entity: str,
) -> pl.DataFrame:
    path = root / entity / f"{entity}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Silver não encontrada: {path}")

    logger.info(
        "Lendo Silver: entidade=%s arquivo=%s",
        entity,
        path,
    )

    return pl.read_parquet(path)


def _write_dataset(
    *,
    dataframe: pl.DataFrame,
    destination_root: Path,
    dataset: str,
    write_csv: bool = True,
) -> dict[str, object]:
    destination = destination_root / dataset

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dataset}.parquet"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    csv_path: Path | None = None

    if write_csv:
        csv_path = destination / f"{dataset}.csv"

        dataframe.write_csv(
            csv_path,
            separator=";",
        )

    logger.info(
        "Gold criada: dataset=%s registros=%s",
        dataset,
        dataframe.height,
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": (str(csv_path) if csv_path is not None else None),
    }


def run_gold_licitacoes(
    *,
    first_period: str,
    last_period: str,
) -> Path:
    period_label = f"{first_period}_{last_period}"

    silver_root = (
        Path("data/silver")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    quality_manifest_path = silver_root / "quality" / "quality.manifest.json"

    if not quality_manifest_path.exists():
        raise FileNotFoundError(
            f"Manifesto de qualidade não encontrado: {quality_manifest_path}"
        )

    quality_manifest = json.loads(quality_manifest_path.read_text(encoding="utf-8"))

    if not quality_manifest.get(
        "approved",
        False,
    ):
        raise ValueError("A Silver de licitações não está aprovada pela qualidade.")

    licitacoes = _read_silver(
        silver_root,
        "licitacoes",
    )

    itens = _read_silver(
        silver_root,
        "itens_licitacao",
    )

    participantes = _read_silver(
        silver_root,
        "participantes_licitacao",
    )

    concorrencia = build_concorrencia_licitacoes(
        licitacoes,
        participantes,
        itens,
    )

    datasets = {
        "concorrencia_licitacoes": concorrencia,
        "ranking_orgaos": build_ranking_orgaos(
            licitacoes,
            concorrencia,
        ),
        "ranking_fornecedores": (build_ranking_fornecedores(itens)),
        "ranking_modalidades": (
            build_ranking_modalidades(
                licitacoes,
                concorrencia,
            )
        ),
        "ranking_uf": build_ranking_uf(licitacoes),
        "relacionamento_orgao_fornecedor": (
            build_relacionamento_orgao_fornecedor(
                licitacoes,
                itens,
            )
        ),
    }

    destination_root = (
        Path("data/gold")
        / "portal_transparencia"
        / "licitacoes"
        / f"periodos={period_label}"
    )

    outputs: list[dict[str, object]] = []

    for dataset, dataframe in datasets.items():
        outputs.append(
            _write_dataset(
                dataframe=dataframe,
                destination_root=(destination_root),
                dataset=dataset,
            )
        )

    manifest = {
        "source": "portal_transparencia",
        "subject": "licitacoes_federais",
        "layer": "gold",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "silver_quality_approved": True,
        "silver_warning_count": (
            quality_manifest.get(
                "warning_count",
                0,
            )
        ),
        "datasets": outputs,
        "methodology_notes": [
            (
                "Os rankings são descritivos e "
                "não representam evidência de "
                "irregularidade."
            ),
            (
                "Os valores de fornecedores são "
                "agregados a partir de Valor Item "
                "publicado pela fonte."
            ),
            ("A fonte pública analisada possui cobertura de 202301 até 202404."),
            (
                "Participações sem item correspondente "
                "foram preservadas na Silver e "
                "registradas como alerta da fonte."
            ),
        ],
    }

    manifest_path = destination_root / "gold.manifest.json"

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
        "Gold de licitações concluída: %s",
        manifest_path,
    )

    return manifest_path
