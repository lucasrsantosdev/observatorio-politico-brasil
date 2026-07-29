from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.proposicoes_votacoes import (
    transform_dataset,
)

logger = logging.getLogger(__name__)


DATASETS = (
    "proposicoes",
    "proposicoes_temas",
    "proposicoes_autores",
    "votacoes",
    "votacoes_orientacoes",
    "votacoes_votos",
    "votacoes_objetos",
    "votacoes_proposicoes",
)


def _read_bronze_file(
    path: Path,
    *,
    year: int,
) -> pl.DataFrame:
    header = pl.read_csv(
        path,
        separator=";",
        encoding="utf8-lossy",
        n_rows=0,
    )

    schema_overrides = {column: pl.String for column in header.columns}

    return (
        pl.read_csv(
            path,
            separator=";",
            encoding="utf8-lossy",
            schema_overrides=schema_overrides,
            infer_schema=False,
            truncate_ragged_lines=False,
        )
        .with_row_index(
            name="linha_arquivo",
            offset=2,
        )
        .with_columns(
            pl.lit(year).cast(pl.Int32).alias("ano_arquivo_origem"),
            pl.lit(path.name).alias("arquivo_origem"),
        )
    )


def _write_dataset(
    *,
    dataframe: pl.DataFrame,
    destination_root: Path,
    dataset: str,
) -> dict[str, object]:
    destination = destination_root / dataset

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

    duplicate_keys = (
        dataframe.group_by("chave_registro").len().filter(pl.col("len") > 1).height
    )

    duplicate_hashes = (
        dataframe.group_by("hash_registro").len().filter(pl.col("len") > 1).height
    )

    logger.info(
        "Silver legislativa criada: dataset=%s registros=%s colunas=%s",
        dataset,
        dataframe.height,
        dataframe.width,
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "distinct_record_keys": dataframe["chave_registro"].n_unique(),
        "duplicate_record_key_groups": (duplicate_keys),
        "duplicate_content_hash_groups": (duplicate_hashes),
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_silver_proposicoes_votacoes(
    *,
    years: list[int],
) -> Path:
    normalized_years = sorted(set(years))

    if not normalized_years:
        raise ValueError("Informe pelo menos um ano.")

    bronze_root = Path("data/bronze") / "camara_deputados" / "proposicoes_votacoes"

    years_label = "_".join(str(year) for year in normalized_years)

    silver_root = (
        Path("data/silver")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    outputs: list[dict[str, object]] = []
    total_source_rows = 0
    total_silver_rows = 0

    for dataset in DATASETS:
        frames: list[pl.DataFrame] = []
        source_files: list[str] = []

        for year in normalized_years:
            year_root = bronze_root / f"dataset={dataset}" / f"ano={year}"

            files = sorted(year_root.glob("*.csv"))

            if not files:
                raise FileNotFoundError(
                    f"Arquivo Bronze não encontrado: dataset={dataset} ano={year}"
                )

            for file_path in files:
                raw = _read_bronze_file(
                    file_path,
                    year=year,
                )

                frames.append(raw)
                source_files.append(str(file_path))
                total_source_rows += raw.height

        consolidated = pl.concat(
            frames,
            how="vertical_relaxed",
        )

        silver = transform_dataset(
            consolidated,
            dataset=dataset,
        )

        total_silver_rows += silver.height

        output = _write_dataset(
            dataframe=silver,
            destination_root=silver_root,
            dataset=dataset,
        )

        output["source_files"] = source_files

        output["source_record_count"] = consolidated.height

        output["record_count_preserved"] = consolidated.height == silver.height

        outputs.append(output)

    all_preserved = all(bool(output["record_count_preserved"]) for output in outputs)

    unique_keys_valid = all(
        output["duplicate_record_key_groups"] == 0 for output in outputs
    )

    manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "layer": "silver",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "dataset_count": len(outputs),
        "source_record_count": (total_source_rows),
        "silver_record_count": (total_silver_rows),
        "all_records_preserved": (all_preserved),
        "unique_record_keys": (unique_keys_valid),
        "approved": (all_preserved and unique_keys_valid),
        "datasets": outputs,
        "methodology_notes": [
            (
                "Todos os campos são lidos inicialmente "
                "como texto para evitar inferência incorreta."
            ),
            ("Campos vazios são normalizados para null."),
            ("IDs, datas, datas-hora e booleanos são convertidos conforme o dataset."),
            (
                "Cada linha física recebe uma chave técnica "
                "composta por dataset, ano, arquivo e linha."
            ),
            (
                "O hash de conteúdo identifica repetições "
                "sem eliminar registros automaticamente."
            ),
        ],
    }

    manifest_path = silver_root / "silver.manifest.json"

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
        "Silver de proposições e votações concluída: "
        "datasets=%s registros=%s aprovado=%s",
        len(outputs),
        total_silver_rows,
        manifest["approved"],
    )

    return manifest_path
