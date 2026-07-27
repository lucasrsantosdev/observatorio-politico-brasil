from __future__ import annotations

import logging
from pathlib import Path

from observatorio_politico.clients.portal_transparencia import (
    PortalTransparenciaClient,
)
from observatorio_politico.config import Settings
from observatorio_politico.extractors.orgaos_siafi import (
    extract_orgaos_siafi,
)
from observatorio_politico.loaders.bronze import save_bronze_json

logger = logging.getLogger(__name__)


def run_orgaos_siafi(settings: Settings) -> tuple[Path, Path]:
    params = {
        "pagina": 1,
    }

    with PortalTransparenciaClient(
        base_url=settings.portal_transparencia_base_url,
        api_key=settings.portal_transparencia_api_key,
        timeout_seconds=settings.request_timeout_seconds,
    ) as client:
        dados = extract_orgaos_siafi(
            client,
            pagina=1,
        )

    data_path, manifest_path = save_bronze_json(
        data=dados,
        bronze_root=settings.bronze_path,
        source="portal_transparencia",
        entity="orgaos_siafi",
        endpoint="/orgaos-siafi",
        request_params=params,
    )

    logger.info(
        "Extração concluída: entidade=orgaos_siafi registros=%s",
        len(dados),
    )
    logger.info("Arquivo Bronze: %s", data_path)
    logger.info("Manifesto: %s", manifest_path)

    return data_path, manifest_path
