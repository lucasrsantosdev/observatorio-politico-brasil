from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import shutil
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


FILE_PATTERN = re.compile(
    r"^(?P<periodo>\d{6})_(?P<entidade>.+)\.csv$",
    re.IGNORECASE,
)

ENTITY_NAMES = {
    "licitacao": "licitacoes",
    "itemlicitacao": "itens_licitacao",
    "participanteslicitacao": "participantes_licitacao",
    "empenhosrelacionados": "empenhos_relacionados",
}


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize(
        "NFKD",
        value,
    )

    without_accents = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )

    return re.sub(
        r"[^a-z0-9]",
        "",
        without_accents.lower(),
    )


def _sha256(file_path: Path) -> str:
    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)

    return digest.hexdigest()


def _inspect_csv(
    file_path: Path,
) -> tuple[int, list[str]]:
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


def run_bronze_licitacoes(
    *,
    landing_root: Path,
    bronze_root: Path = Path("data/bronze"),
) -> Path:
    if not landing_root.exists():
        raise FileNotFoundError(f"Landing de licitações não encontrada: {landing_root}")

    execution_time = datetime.now(UTC)
    execution_id = execution_time.strftime("%Y%m%dT%H%M%SZ")

    csv_files = sorted(landing_root.rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em: {landing_root}")

    processed_files: list[dict[str, object]] = []
    ignored_files: list[dict[str, str]] = []

    layouts: dict[str, dict[str, object]] = {}

    for source_file in csv_files:
        match = FILE_PATTERN.match(source_file.name)

        if match is None:
            ignored_files.append(
                {
                    "file": str(source_file),
                    "reason": "nome_fora_do_padrao",
                }
            )
            continue

        periodo = match.group("periodo")
        source_entity = match.group("entidade")
        normalized_entity = _normalize_name(source_entity)

        entity = ENTITY_NAMES.get(normalized_entity)

        if entity is None:
            ignored_files.append(
                {
                    "file": str(source_file),
                    "reason": (f"entidade_nao_reconhecida:{source_entity}"),
                }
            )

            logger.warning(
                "Entidade não reconhecida: arquivo=%s entidade=%s",
                source_file,
                source_entity,
            )
            continue

        year = int(periodo[:4])
        month = int(periodo[4:6])

        record_count, columns = _inspect_csv(source_file)

        layout_signature = hashlib.sha256("|".join(columns).encode("utf-8")).hexdigest()

        layouts.setdefault(
            layout_signature,
            {
                "entity": entity,
                "column_count": len(columns),
                "columns": columns,
                "files": 0,
            },
        )

        layouts[layout_signature]["files"] = int(layouts[layout_signature]["files"]) + 1

        destination = (
            bronze_root
            / "portal_transparencia"
            / "licitacoes"
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
            "source_entity": source_entity,
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
            "layout_signature": layout_signature,
            "sha256": _sha256(destination_file),
            "file_size_bytes": (destination_file.stat().st_size),
            "empty_file": record_count == 0,
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

    totals_by_entity: dict[
        str,
        dict[str, int],
    ] = {}

    periods: set[str] = set()

    for metadata in processed_files:
        entity = str(metadata["entity"])
        periodo = str(metadata["periodo"])

        periods.add(periodo)

        totals_by_entity.setdefault(
            entity,
            {
                "files": 0,
                "records": 0,
                "empty_files": 0,
            },
        )

        totals_by_entity[entity]["files"] += 1
        totals_by_entity[entity]["records"] += int(metadata["record_count"])

        if bool(metadata["empty_file"]):
            totals_by_entity[entity]["empty_files"] += 1

    manifest_root = (
        bronze_root
        / "portal_transparencia"
        / "licitacoes"
        / "_control"
        / f"execucao={execution_id}"
    )

    manifest_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    ordered_periods = sorted(periods)

    manifest = {
        "source": "portal_transparencia",
        "subject": "licitacoes_federais",
        "layer": "bronze",
        "execution_id": execution_id,
        "processed_at_utc": (execution_time.isoformat()),
        "landing_root": str(landing_root),
        "processed_file_count": len(processed_files),
        "ignored_file_count": len(ignored_files),
        "period_count": len(ordered_periods),
        "first_period": (ordered_periods[0] if ordered_periods else None),
        "last_period": (ordered_periods[-1] if ordered_periods else None),
        "source_outdated": (bool(ordered_periods) and ordered_periods[-1] < "202405"),
        "totals_by_entity": totals_by_entity,
        "layouts": list(layouts.values()),
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
        "Bronze de licitações concluída: arquivos=%s períodos=%s manifesto=%s",
        len(processed_files),
        len(ordered_periods),
        manifest_path,
    )

    return manifest_path
