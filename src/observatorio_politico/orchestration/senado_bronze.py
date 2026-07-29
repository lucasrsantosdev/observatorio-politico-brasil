from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


SOURCES: tuple[dict[str, Any], ...] = (
    {
        "dataset": "senadores",
        "file_name": "senadores_atuais.json",
        "url": ("https://legis.senado.leg.br/dadosabertos/senador/lista/atual"),
        "year": None,
    },
    {
        "dataset": "materias",
        "file_name": "materias.json",
        "url": ("https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista"),
        "year": None,
    },
    {
        "dataset": "votacoes",
        "file_name": "votacoes.json",
        "url": ("https://legis.senado.leg.br/dadosabertos/votacao"),
        "year": None,
    },
    {
        "dataset": "ceaps",
        "file_name": "ceaps_2025.csv",
        "url": (
            "https://adm.senado.gov.br/"
            "adm-dadosabertos/api/v1/senadores/"
            "despesas_ceaps/2025/csv"
        ),
        "year": 2025,
    },
    {
        "dataset": "ceaps",
        "file_name": "ceaps_2026.csv",
        "url": (
            "https://adm.senado.gov.br/"
            "adm-dadosabertos/api/v1/senadores/"
            "despesas_ceaps/2026/csv"
        ),
        "year": 2026,
    },
    {
        "dataset": "empresas_contratadas",
        "file_name": "empresas_contratadas.csv",
        "url": (
            "https://adm.senado.leg.br/"
            "adm-dadosabertos/api/v1/"
            "contratacoes/empresas/csv"
        ),
        "year": None,
    },
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _download(
    client: httpx.Client,
    source: dict[str, Any],
    bronze_root: Path,
) -> dict[str, Any]:
    response = client.get(
        source["url"],
    )

    response.raise_for_status()

    dataset = str(source["dataset"])
    year = source["year"]

    destination = bronze_root / f"dataset={dataset}"

    if year is not None:
        destination = destination / f"ano={year}"

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = destination / str(source["file_name"])

    content = response.content

    output_path.write_bytes(content)

    record = {
        "dataset": dataset,
        "year": year,
        "url": source["url"],
        "final_url": str(response.url),
        "status_code": response.status_code,
        "content_type": response.headers.get(
            "content-type",
            "",
        ),
        "content_length": len(content),
        "sha256": _sha256(content),
        "file": str(output_path),
        "exists": output_path.exists(),
    }

    logger.info(
        "Bronze Senado criada: dataset=%s ano=%s bytes=%s arquivo=%s",
        dataset,
        year,
        len(content),
        output_path,
    )

    return record


def run_senado_bronze() -> Path:
    bronze_root = Path("data/bronze") / "senado_federal"

    bronze_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    headers = {
        "User-Agent": ("ObservatorioPoliticoBrasil/1.0"),
        "Accept": ("application/json, text/csv, text/plain, */*"),
    }

    with httpx.Client(
        headers=headers,
        timeout=120,
        follow_redirects=True,
    ) as client:
        outputs = [
            _download(
                client,
                source,
                bronze_root,
            )
            for source in SOURCES
        ]

    manifest = {
        "source": "senado_federal",
        "layer": "bronze",
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "source_count": len(SOURCES),
        "file_count": len(outputs),
        "total_bytes": sum(int(item["content_length"]) for item in outputs),
        "all_files_created": all(bool(item["exists"]) for item in outputs),
        "approved": (
            len(outputs) == len(SOURCES)
            and all(
                bool(item["exists"]) and int(item["content_length"]) > 0
                for item in outputs
            )
        ),
        "files": outputs,
        "methodology_notes": [
            ("Os arquivos da Bronze preservam os bytes recebidos das fontes oficiais."),
            ("Nenhuma normalizacao ou exclusao de registros e realizada nesta camada."),
            (
                "O hash SHA-256 permite verificar integridade "
                "e alteracoes futuras da fonte."
            ),
        ],
    }

    manifest_path = bronze_root / "bronze.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("BRONZE DO SENADO FEDERAL")
    print("=" * 100)

    for item in outputs:
        print(
            f"{item['dataset']:<25} "
            f"ano={item['year']!s:<6} "
            f"bytes={item['content_length']:<10} "
            f"arquivo={item['file']}"
        )

    print()
    print(f"ARQUIVOS_CRIADOS={len(outputs)}")
    print(f"TOTAL_BYTES={manifest['total_bytes']}")
    print(f"BRONZE_APPROVED={manifest['approved']}")
    print(f"MANIFESTO={manifest_path}")

    return manifest_path
