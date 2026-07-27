from __future__ import annotations

from typing import Any

from observatorio_politico.clients.portal_transparencia import (
    PortalTransparenciaClient,
)


def extract_orgaos_siafi(
    client: PortalTransparenciaClient,
    *,
    pagina: int = 1,
) -> list[dict[str, Any]]:
    resultado = client.get(
        "/orgaos-siafi",
        params={
            "pagina": pagina,
        },
    )

    if not isinstance(resultado, list):
        raise TypeError(
            "O endpoint /orgaos-siafi não retornou uma lista."
        )

    return resultado
