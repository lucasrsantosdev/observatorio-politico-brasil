from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from observatorio_politico.clients.portal_transparencia import (
    PortalTransparenciaClient,
)
from observatorio_politico.config import Settings
from observatorio_politico.extractors.emendas import (
    extract_emendas_page,
)

logger = logging.getLogger(__name__)


def _save_page(
    *,
    dados: list[dict[str, Any]],
    destination: Path,
    pagina: int,
) -> tuple[Path, str]:
    payload = json.dumps(
        dados,
        ensure_ascii=False,
        indent=2,
        default=str,
    )

    sha256 = hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()

    file_path = destination / f"pagina_{pagina:05d}.json"

    file_path.write_text(
        payload,
        encoding="utf-8",
    )

    return file_path, sha256


def run_emendas(
    settings: Settings,
    *,
    ano: int,
    max_pages: int | None = None,
) -> Path:
    if max_pages is not None and max_pages < 1:
        raise ValueError("max_pages deve ser maior ou igual a 1.")

    execution_time = datetime.now(UTC)
    execution_id = execution_time.strftime("%Y%m%dT%H%M%SZ")

    destination = (
        settings.bronze_path
        / "portal_transparencia"
        / "emendas"
        / f"ano_emenda={ano}"
        / f"data_extracao={execution_time:%Y-%m-%d}"
        / f"execucao={execution_id}"
    )
    destination.mkdir(parents=True, exist_ok=True)

    paginas_processadas: list[dict[str, Any]] = []
    total_registros = 0
    pagina = 1

    with PortalTransparenciaClient(
        base_url=settings.portal_transparencia_base_url,
        api_key=settings.portal_transparencia_api_key,
        timeout_seconds=settings.request_timeout_seconds,
    ) as client:
        while True:
            if max_pages is not None and pagina > max_pages:
                logger.info(
                    "Limite de páginas atingido: max_pages=%s",
                    max_pages,
                )
                break

            logger.info(
                "Extraindo emendas: ano=%s pagina=%s",
                ano,
                pagina,
            )

            dados = extract_emendas_page(
                client,
                ano=ano,
                pagina=pagina,
            )

            if not dados:
                logger.info(
                    "Página vazia encontrada: ano=%s pagina=%s",
                    ano,
                    pagina,
                )
                break

            arquivo, sha256 = _save_page(
                dados=dados,
                destination=destination,
                pagina=pagina,
            )

            quantidade = len(dados)
            total_registros += quantidade

            paginas_processadas.append(
                {
                    "pagina": pagina,
                    "record_count": quantidade,
                    "sha256": sha256,
                    "data_file": arquivo.name,
                }
            )

            logger.info(
                "Página salva: pagina=%s registros=%s arquivo=%s",
                pagina,
                quantidade,
                arquivo,
            )

            pagina += 1

    manifest = {
        "source": "portal_transparencia",
        "entity": "emendas",
        "endpoint": "/emendas",
        "ano_emenda": ano,
        "execution_id": execution_id,
        "extracted_at_utc": execution_time.isoformat(),
        "max_pages": max_pages,
        "pages_processed": len(paginas_processadas),
        "record_count": total_registros,
        "pages": paginas_processadas,
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
        "Extração de emendas concluída: ano=%s paginas=%s registros=%s",
        ano,
        len(paginas_processadas),
        total_registros,
    )
    logger.info("Manifesto consolidado: %s", manifest_path)

    return manifest_path
