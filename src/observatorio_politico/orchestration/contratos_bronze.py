from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


FILE_PATTERN = re.compile(
    r"^(?P<periodo>\d{6})_"
    r"(?P<entidade>Compras|ItemCompra|TermoAditivo|Apostilamento)"
    r"\.csv$",
    re.IGNORECASE,
)

ENTITY_NAMES = {
    "compras": "contratos",
    "itemcompra": "itens_contrato",
    "termoaditivo": "termos_aditivos",
    "apostilamento": "apostilamentos",
}


def _sha256(file_path: Path) -> str:
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)

    return digest.hexdigest()


def _inspect_csv(file_path: Path) -> tuple[int, list[str]]:
    with file_path.open(
        "r",
        encoding="cp1252",
        newline="",
    ) as file:
        reader = csv.reader(
            file,
            delimiter=";",
        )

        header = next(reader, [])
        record_count = sum(1 for _ in reader)

    return record_count, header


def run_bronze_contratos(
    *,
    landing_root: Path,
    bronze_root: Path = Path("data/bronze"),
) -> Path:
    if not landing_root.exists():
        raise FileNotFoundError(f"Landing de contratos não encontrada: {landing_root}")

    execution_time = datetime.now(UTC)
    execution_id = execution_time.strftime("%Y%m%dT%H%M%SZ")

    csv_files = sorted(landing_root.rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {landing_root}")

    processed_files: list[dict[str, object]] = []
    ignored_files: list[str] = []

    for source_file in csv_files:
        match = FILE_PATTERN.match(source_file.name)

        if match is None:
            ignored_files.append(str(source_file))
            logger.warning(
                "Arquivo ignorado por nome não reconhecido: %s",
                source_file,
            )
            continue

        periodo = match.group("periodo")
        source_entity = match.group("entidade").lower()
        entity = ENTITY_NAMES[source_entity]

        year = int(periodo[:4])
        month = int(periodo[4:6])

        record_count, columns = _inspect_csv(source_file)

        destination = (
            bronze_root
            / "portal_transparencia"
            / "contratos"
            / entity
            / f"ano={year}"
            / f"mes={month:02d}"
            / f"execucao={execution_id}"
        )
        destination.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination_file = destination / source_file.name

        shutil.copy2(
            source_file,
            destination_file,
        )

        metadata = {
            "entity": entity,
            "source_entity": match.group("entidade"),
            "periodo": periodo,
            "ano": year,
            "mes": month,
            "source_file": str(source_file),
            "bronze_file": str(destination_file),
            "encoding": "cp1252",
            "delimiter": ";",
            "record_count": record_count,
            "column_count": len(columns),
            "columns": columns,
            "sha256": _sha256(destination_file),
            "file_size_bytes": destination_file.stat().st_size,
        }
        processed_files.append(metadata)

        logger.info(
            "Bronze criada: entidade=%s periodo=%s registros=%s arquivo=%s",
            entity,
            periodo,
            record_count,
            destination_file,
        )

    if not processed_files:
        raise ValueError("Nenhum arquivo válido foi processado.")

    manifest_root = (
        bronze_root
        / "portal_transparencia"
        / "contratos"
        / "_control"
        / f"execucao={execution_id}"
    )
    manifest_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    totals_by_entity: dict[str, dict[str, int]] = {}

    for metadata in processed_files:
        entity = str(metadata["entity"])

        totals_by_entity.setdefault(
            entity,
            {
                "files": 0,
                "records": 0,
            },
        )

        totals_by_entity[entity]["files"] += 1
        totals_by_entity[entity]["records"] += int(metadata["record_count"])

    manifest = {
        "source": "portal_transparencia",
        "subject": "contratos_federais",
        "layer": "bronze",
        "execution_id": execution_id,
        "processed_at_utc": execution_time.isoformat(),
        "landing_root": str(landing_root),
        "processed_file_count": len(processed_files),
        "ignored_file_count": len(ignored_files),
        "totals_by_entity": totals_by_entity,
        "files": processed_files,
        "ignored_files": ignored_files,
    }

    manifest_path = manifest_root / "execucao.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Bronze de contratos concluída: arquivos=%s manifesto=%s",
        len(processed_files),
        manifest_path,
    )

    return manifest_path
