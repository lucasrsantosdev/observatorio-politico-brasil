from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from observatorio_politico.transformations.contratos import (
    transform_apostilamentos,
    transform_contratos,
    transform_itens_contrato,
    transform_termos_aditivos,
)

logger = logging.getLogger(__name__)

TransformFunction = Callable[..., pl.DataFrame]


ENTITY_CONFIG = {
    "contratos": {
        "transform": transform_contratos,
        "key": "chave_registro_contrato",
    },
    "itens_contrato": {
        "transform": transform_itens_contrato,
        "key": "chave_item_contrato",
    },
    "termos_aditivos": {
        "transform": transform_termos_aditivos,
        "key": "chave_termo_aditivo",
    },
    "apostilamentos": {
        "transform": transform_apostilamentos,
        "key": "chave_apostilamento",
    },
}


def _latest_bronze_manifest(
    bronze_root: Path,
) -> Path:
    manifests = sorted(
        (bronze_root / "_control").rglob("execucao.manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not manifests:
        raise FileNotFoundError(
            f"Manifesto Bronze de contratos não encontrado em: {bronze_root}"
        )

    return manifests[0]


def _read_csv(
    path: Path,
) -> pl.DataFrame:
    return pl.read_csv(
        path,
        separator=";",
        encoding="windows-1252",
        infer_schema=False,
        ignore_errors=False,
        null_values=[],
    )


def _load_entity(
    *,
    files: list[dict[str, object]],
    transform: TransformFunction,
) -> tuple[
    pl.DataFrame,
    list[dict[str, object]],
]:
    frames: list[pl.DataFrame] = []
    empty_schema_frame: pl.DataFrame | None = None

    source_control: list[dict[str, object]] = []

    for metadata in sorted(
        files,
        key=lambda item: (
            str(item["periodo"]),
            str(item["bronze_file"]),
        ),
    ):
        path = Path(str(metadata["bronze_file"]))

        periodo = str(metadata["periodo"])

        raw = _read_csv(path)
        input_rows = raw.height

        transformed = transform(
            raw,
            periodo=periodo,
            source_file=str(path),
        )

        source_control.append(
            {
                "file": str(path),
                "period": periodo,
                "input_rows": input_rows,
                "output_rows": transformed.height,
            }
        )

        if transformed.height > 0:
            frames.append(transformed)
        elif empty_schema_frame is None:
            empty_schema_frame = transformed.head(0)

        logger.info(
            "Arquivo transformado: entidade=%s periodo=%s registros=%s",
            metadata["entity"],
            periodo,
            transformed.height,
        )

    if frames:
        return (
            pl.concat(
                frames,
                how="vertical_relaxed",
                rechunk=True,
            ),
            source_control,
        )

    if empty_schema_frame is not None:
        return (
            empty_schema_frame,
            source_control,
        )

    return (
        pl.DataFrame(),
        source_control,
    )


def _deduplicate(
    dataframe: pl.DataFrame,
    *,
    key: str,
) -> tuple[pl.DataFrame, int]:
    if dataframe.is_empty():
        return dataframe, 0

    input_rows = dataframe.height

    output = dataframe.sort(
        [
            key,
            "periodo_origem",
        ],
        descending=[
            False,
            True,
        ],
    ).unique(
        subset=[key],
        keep="first",
        maintain_order=True,
    )

    return (
        output,
        input_rows - output.height,
    )


def _build_current_contracts(
    contracts_history: pl.DataFrame,
) -> pl.DataFrame:
    if contracts_history.is_empty():
        return contracts_history

    latest_periods = contracts_history.group_by("chave_grupo_contrato").agg(
        pl.col("periodo_origem").max().alias("periodo_mais_recente")
    )

    return (
        contracts_history.join(
            latest_periods,
            on="chave_grupo_contrato",
            how="left",
        )
        .filter(pl.col("periodo_origem") == pl.col("periodo_mais_recente"))
        .with_columns(pl.lit(True).alias("registro_periodo_atual"))
        .sort(
            [
                "chave_grupo_contrato",
                "chave_registro_contrato",
            ]
        )
    )


def _write_dataset(
    *,
    dataframe: pl.DataFrame,
    root: Path,
    entity: str,
) -> dict[str, object]:
    destination = root / entity

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
    )

    logger.info(
        "Silver criada: entidade=%s registros=%s",
        entity,
        dataframe.height,
    )

    return {
        "entity": entity,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_silver_contratos(
    *,
    first_period: str,
    last_period: str,
    bronze_root: Path = Path("data/bronze/portal_transparencia/contratos"),
    silver_root: Path = Path("data/silver/portal_transparencia/contratos"),
) -> Path:
    manifest_path = _latest_bronze_manifest(bronze_root)

    bronze_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    selected_files = [
        metadata
        for metadata in bronze_manifest["files"]
        if (first_period <= str(metadata["periodo"]) <= last_period)
    ]

    files_by_entity: dict[
        str,
        list[dict[str, object]],
    ] = {}

    for metadata in selected_files:
        entity = str(metadata["entity"])

        files_by_entity.setdefault(
            entity,
            [],
        ).append(metadata)

    destination_root = silver_root / (f"periodos={first_period}_{last_period}")

    transformed: dict[
        str,
        pl.DataFrame,
    ] = {}

    control: dict[
        str,
        list[dict[str, object]],
    ] = {}

    deduplication: dict[
        str,
        int,
    ] = {}

    for entity, config in ENTITY_CONFIG.items():
        dataframe, source_control = _load_entity(
            files=files_by_entity.get(
                entity,
                [],
            ),
            transform=config["transform"],
        )

        dataframe, removed = _deduplicate(
            dataframe,
            key=str(config["key"]),
        )

        transformed[entity] = dataframe
        control[entity] = source_control
        deduplication[entity] = removed

    contracts_history = transformed["contratos"]

    contracts_current = _build_current_contracts(contracts_history)

    outputs = [
        _write_dataset(
            dataframe=contracts_history,
            root=destination_root,
            entity="contratos_historico",
        ),
        _write_dataset(
            dataframe=contracts_current,
            root=destination_root,
            entity="contratos_atuais",
        ),
        _write_dataset(
            dataframe=transformed["itens_contrato"],
            root=destination_root,
            entity="itens_contrato",
        ),
        _write_dataset(
            dataframe=transformed["termos_aditivos"],
            root=destination_root,
            entity="termos_aditivos",
        ),
        _write_dataset(
            dataframe=transformed["apostilamentos"],
            root=destination_root,
            entity="apostilamentos",
        ),
    ]

    manifest = {
        "source": "portal_transparencia",
        "subject": "contratos_federais",
        "layer": "silver",
        "first_period": first_period,
        "last_period": last_period,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "bronze_manifest": str(manifest_path),
        "datasets": outputs,
        "duplicates_removed": (deduplication),
        "source_files": control,
        "methodology_notes": [
            (
                "Código UG e número do contrato "
                "formam uma chave de agrupamento, "
                "não uma chave física única."
            ),
            (
                "Registros distintos do mesmo "
                "grupo foram preservados por hash "
                "de conteúdo."
            ),
            (
                "Contratos atuais mantêm todos os "
                "registros distintos existentes no "
                "último período de cada grupo."
            ),
            (
                "Itens de código zero foram "
                "preservados separadamente quando "
                "quantidade ou valor diferem."
            ),
        ],
    }

    manifest_path = destination_root / "silver.manifest.json"

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
        "Silver de contratos concluída: %s",
        manifest_path,
    )

    return manifest_path
