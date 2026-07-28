from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.licitacoes import (
    transform_empenhos_relacionados,
    transform_itens_licitacao,
    transform_licitacoes,
    transform_participantes_licitacao,
)

logger = logging.getLogger(__name__)


PERIOD_PATTERN = re.compile(r"^(?P<periodo>\d{6})_")

ENTITY_CONFIG = {
    "licitacoes": {
        "pattern": "*_Licitação.csv",
        "transform": transform_licitacoes,
        "deduplication_key": [
            "chave_licitacao",
        ],
    },
    "itens_licitacao": {
        "pattern": "*_ItemLicitação.csv",
        "transform": transform_itens_licitacao,
        "deduplication_key": [
            "chave_item_vencedor",
        ],
    },
    "participantes_licitacao": {
        "pattern": "*_ParticipantesLicitação.csv",
        "transform": transform_participantes_licitacao,
        "deduplication_key": [
            "chave_participacao",
        ],
    },
    "empenhos_relacionados": {
        "pattern": "*_EmpenhosRelacionados.csv",
        "transform": transform_empenhos_relacionados,
        "deduplication_key": [
            "chave_empenho_licitacao",
        ],
    },
}


TransformFunction = Callable[..., pl.DataFrame]


def _read_csv_as_strings(
    path: Path,
) -> pl.DataFrame:
    return pl.read_csv(
        path,
        separator=";",
        encoding="windows-1252",
        infer_schema=False,
        ignore_errors=False,
        truncate_ragged_lines=False,
        null_values=[],
    )


def _extract_period(
    path: Path,
) -> str:
    match = PERIOD_PATTERN.match(path.name)

    if match is None:
        raise ValueError(f"Período não identificado no arquivo: {path}")

    return match.group("periodo")


def _load_entity(
    *,
    bronze_root: Path,
    entity: str,
    pattern: str,
    transform: TransformFunction,
    periods: set[str],
) -> tuple[pl.DataFrame, list[dict[str, object]]]:
    entity_root = bronze_root / entity

    files = sorted(entity_root.rglob(pattern))

    files = [path for path in files if _extract_period(path) in periods]

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado para {entity}: {entity_root} / {pattern}"
        )

    frames: list[pl.DataFrame] = []
    source_files: list[dict[str, object]] = []

    for path in files:
        periodo = _extract_period(path)

        raw = _read_csv_as_strings(path)

        input_rows = raw.height

        if input_rows == 0:
            logger.info(
                "Arquivo vazio mantido no controle: entidade=%s periodo=%s arquivo=%s",
                entity,
                periodo,
                path,
            )

            source_files.append(
                {
                    "file": str(path),
                    "period": periodo,
                    "input_rows": 0,
                    "output_rows": 0,
                }
            )
            continue

        transformed = transform(
            raw,
            periodo=periodo,
            source_file=str(path),
        )

        frames.append(transformed)

        source_files.append(
            {
                "file": str(path),
                "period": periodo,
                "input_rows": input_rows,
                "output_rows": transformed.height,
            }
        )

        logger.info(
            "Arquivo transformado: entidade=%s periodo=%s registros=%s",
            entity,
            periodo,
            transformed.height,
        )

    if not frames:
        return pl.DataFrame(), source_files

    return (
        pl.concat(
            frames,
            how="vertical_relaxed",
            rechunk=True,
        ),
        source_files,
    )


def _deduplicate_latest(
    dataframe: pl.DataFrame,
    *,
    key: list[str],
) -> tuple[pl.DataFrame, int]:
    if dataframe.is_empty():
        return dataframe, 0

    input_rows = dataframe.height

    output = dataframe.sort(
        [
            *key,
            "periodo_origem",
        ],
        descending=[
            *([False] * len(key)),
            True,
        ],
    ).unique(
        subset=key,
        keep="first",
        maintain_order=True,
    )

    duplicates_removed = input_rows - output.height

    return output, duplicates_removed


def _write_dataset(
    *,
    dataframe: pl.DataFrame,
    destination: Path,
    entity: str,
) -> dict[str, object]:
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{entity}.parquet"

    csv_path = destination / f"{entity}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
        include_header=True,
    )

    return {
        "entity": entity,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_silver_licitacoes(
    *,
    periods: list[str],
    bronze_root: Path = Path("data/bronze/portal_transparencia/licitacoes"),
    silver_root: Path = Path("data/silver/portal_transparencia/licitacoes"),
) -> Path:
    ordered_periods = sorted(set(periods))

    if not ordered_periods:
        raise ValueError("Informe ao menos um período.")

    for period in ordered_periods:
        if not re.fullmatch(
            r"\d{6}",
            period,
        ):
            raise ValueError(f"Período inválido: {period}")

    periods_set = set(ordered_periods)

    period_label = f"{ordered_periods[0]}_{ordered_periods[-1]}"

    destination_root = silver_root / f"periodos={period_label}"

    execution_time = datetime.now(UTC)

    outputs: list[dict[str, object]] = []
    files_manifest: dict[
        str,
        list[dict[str, object]],
    ] = {}

    for entity, config in ENTITY_CONFIG.items():
        dataframe, source_files = _load_entity(
            bronze_root=bronze_root,
            entity=entity,
            pattern=str(config["pattern"]),
            transform=config["transform"],
            periods=periods_set,
        )

        input_rows = dataframe.height

        dataframe, duplicates_removed = _deduplicate_latest(
            dataframe,
            key=list(config["deduplication_key"]),
        )

        output = _write_dataset(
            dataframe=dataframe,
            destination=(destination_root / entity),
            entity=entity,
        )

        output["input_rows"] = input_rows
        output["duplicates_removed"] = duplicates_removed

        outputs.append(output)
        files_manifest[entity] = source_files

        logger.info(
            "Silver criada: entidade=%s entrada=%s saída=%s duplicados_removidos=%s",
            entity,
            input_rows,
            dataframe.height,
            duplicates_removed,
        )

    manifest = {
        "source": "portal_transparencia",
        "subject": "licitacoes_federais",
        "layer": "silver",
        "processed_at_utc": (execution_time.isoformat()),
        "periods": ordered_periods,
        "first_period": ordered_periods[0],
        "last_period": ordered_periods[-1],
        "source_outdated": (ordered_periods[-1] < "202405"),
        "datasets": outputs,
        "source_files": files_manifest,
    }

    manifest_path = destination_root / "silver.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Silver de licitações concluída: %s",
        manifest_path,
    )

    return manifest_path
