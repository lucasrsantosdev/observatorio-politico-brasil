from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

OUTPUT_PATH = Path("output/auditoria/senado/inventario_fontes.json")


SOURCES: list[dict[str, str]] = [
    {
        "dataset": "senadores",
        "grupo": "legislativo",
        "url": ("https://legis.senado.leg.br/dadosabertos/senador/lista/atual"),
    },
    {
        "dataset": "materias",
        "grupo": "legislativo",
        "url": ("https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista"),
    },
    {
        "dataset": "votacoes",
        "grupo": "legislativo",
        "url": ("https://legis.senado.leg.br/dadosabertos/votacao"),
    },
    {
        "dataset": "ceaps_2025",
        "grupo": "administrativo",
        "url": (
            "https://adm.senado.gov.br/"
            "adm-dadosabertos/api/v1/senadores/"
            "despesas_ceaps/2025/csv"
        ),
    },
    {
        "dataset": "ceaps_2026",
        "grupo": "administrativo",
        "url": (
            "https://adm.senado.gov.br/"
            "adm-dadosabertos/api/v1/senadores/"
            "despesas_ceaps/2026/csv"
        ),
    },
    {
        "dataset": "empresas_contratadas",
        "grupo": "administrativo",
        "url": (
            "https://adm.senado.leg.br/"
            "adm-dadosabertos/api/v1/"
            "contratacoes/empresas/csv"
        ),
    },
]


def inspect_source(
    client: httpx.Client,
    source: dict[str, str],
) -> dict[str, Any]:
    url = source["url"]

    try:
        response = client.get(url)

        content_type = response.headers.get(
            "content-type",
            "",
        )

        preview = response.text[:500]

        return {
            **source,
            "status_code": response.status_code,
            "ok": response.is_success,
            "content_type": content_type,
            "content_length": len(response.content),
            "final_url": str(response.url),
            "preview": preview,
            "error": None,
        }

    except httpx.HTTPError as exc:
        return {
            **source,
            "status_code": None,
            "ok": False,
            "content_type": None,
            "content_length": 0,
            "final_url": None,
            "preview": None,
            "error": (f"{type(exc).__name__}: {exc}"),
        }


def main() -> None:
    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    headers = {
        "User-Agent": ("ObservatorioPoliticoBrasil/1.0"),
        "Accept": ("application/json, application/xml, text/csv, text/plain, */*"),
    }

    with httpx.Client(
        headers=headers,
        timeout=60,
        follow_redirects=True,
    ) as client:
        results = [
            inspect_source(
                client,
                source,
            )
            for source in SOURCES
        ]

    manifest = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "source_count": len(results),
        "approved_count": sum(bool(item["ok"]) for item in results),
        "sources": results,
    }

    OUTPUT_PATH.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("INVENTARIO DE FONTES DO SENADO")
    print("=" * 100)

    for item in results:
        status = "OK" if item["ok"] else "FALHOU"

        print(
            f"{status:<7} "
            f"{item['dataset']:<25} "
            f"status={item['status_code']} "
            f"bytes={item['content_length']} "
            f"tipo={item['content_type']}"
        )

        if item["error"]:
            print(f"        erro={item['error']}")

    print()
    print(f"FONTES_OK={manifest['approved_count']}/{manifest['source_count']}")

    print(f"INVENTARIO={OUTPUT_PATH}")


if __name__ == "__main__":
    main()
