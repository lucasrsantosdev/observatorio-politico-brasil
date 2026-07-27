from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
import zipfile
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def calculate_sha256(file_path: Path) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest()


def detect_encoding(file_path: Path) -> str:
    encodings = (
        "utf-8-sig",
        "cp1252",
        "latin-1",
    )

    sample = file_path.read_bytes()[:100_000]

    for encoding in encodings:
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue

    raise UnicodeError(f"Não foi possível identificar a codificação de {file_path}.")


def inspect_csv(
    file_path: Path,
    *,
    encoding: str,
) -> dict[str, object]:
    with file_path.open(
        "r",
        encoding=encoding,
        newline="",
        errors="strict",
    ) as file:
        reader = csv.reader(
            file,
            delimiter=";",
        )

        header = next(reader, [])
        record_count = sum(1 for _ in reader)

    return {
        "file": file_path.name,
        "encoding": encoding,
        "delimiter": ";",
        "record_count": record_count,
        "column_count": len(header),
        "columns": header,
        "sha256": calculate_sha256(file_path),
    }


def import_historical_file(
    *,
    source_file: Path,
    bronze_root: Path,
    entity: str,
    ano: int | None,
) -> Path:
    if not source_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {source_file}")

    execution_time = datetime.now(UTC)
    execution_id = execution_time.strftime("%Y%m%dT%H%M%SZ")

    destination = (
        bronze_root
        / "portal_transparencia"
        / f"{entity}_historico"
        / (f"ano={ano}" if ano is not None else "todos_os_anos")
        / f"execucao={execution_id}"
    )
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    original_sha256 = calculate_sha256(source_file)

    original_destination = destination / f"original_{source_file.name}"
    shutil.copy2(
        source_file,
        original_destination,
    )

    extracted_directory = destination / "arquivos_extraidos"
    extracted_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    if zipfile.is_zipfile(source_file):
        with zipfile.ZipFile(source_file) as zip_file:
            zip_file.extractall(extracted_directory)
    elif source_file.suffix.lower() == ".csv":
        shutil.copy2(
            source_file,
            extracted_directory / source_file.name,
        )
    else:
        raise ValueError("Formato não suportado. Utilize um arquivo ZIP ou CSV.")

    csv_files = sorted(extracted_directory.rglob("*.csv"))

    if not csv_files:
        raise FileNotFoundError("O pacote não contém arquivos CSV.")

    files_metadata: list[dict[str, object]] = []

    for csv_file in csv_files:
        encoding = detect_encoding(csv_file)

        metadata = inspect_csv(
            csv_file,
            encoding=encoding,
        )
        files_metadata.append(metadata)

        logger.info(
            "CSV identificado: arquivo=%s registros=%s colunas=%s encoding=%s",
            csv_file.name,
            metadata["record_count"],
            metadata["column_count"],
            encoding,
        )

    manifest = {
        "source": "portal_transparencia",
        "entity": entity,
        "layer": "bronze",
        "load_type": "historical_file",
        "ano": ano,
        "execution_id": execution_id,
        "imported_at_utc": execution_time.isoformat(),
        "original_file": source_file.name,
        "original_sha256": original_sha256,
        "csv_file_count": len(csv_files),
        "files": files_metadata,
    }

    manifest_path = destination / "execucao.manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Importação histórica concluída: entidade=%s ano=%s arquivos=%s",
        entity,
        ano,
        len(csv_files),
    )
    logger.info(
        "Manifesto histórico: %s",
        manifest_path,
    )

    return manifest_path
