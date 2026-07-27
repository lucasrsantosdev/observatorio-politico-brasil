from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def save_bronze_json(
    *,
    data: Any,
    bronze_root: Path,
    source: str,
    entity: str,
    endpoint: str,
    request_params: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    execution_time = datetime.now(UTC)

    destination = (
        bronze_root
        / source
        / entity
        / f"ano={execution_time:%Y}"
        / f"mes={execution_time:%m}"
        / f"dia={execution_time:%d}"
    )
    destination.mkdir(parents=True, exist_ok=True)

    payload = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=str,
    )

    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    timestamp = execution_time.strftime("%Y%m%dT%H%M%SZ")
    data_filename = f"{entity}_{timestamp}_{payload_hash[:12]}.json"
    manifest_filename = f"{entity}_{timestamp}_{payload_hash[:12]}.manifest.json"

    data_path = destination / data_filename
    manifest_path = destination / manifest_filename

    data_path.write_text(
        payload,
        encoding="utf-8",
    )

    if isinstance(data, list):
        record_count = len(data)
    elif data is None:
        record_count = 0
    else:
        record_count = 1

    manifest = {
        "source": source,
        "entity": entity,
        "endpoint": endpoint,
        "request_params": request_params or {},
        "extracted_at_utc": execution_time.isoformat(),
        "record_count": record_count,
        "sha256": payload_hash,
        "data_file": data_filename,
    }

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return data_path, manifest_path
