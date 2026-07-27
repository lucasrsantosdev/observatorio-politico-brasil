from __future__ import annotations

from typing import Any

from observatorio_politico.clients.portal_transparencia import (
    PortalTransparenciaClient,
)


def extract_emendas_page(
    client: PortalTransparenciaClient,
    *,
    ano: int,
    pagina: int,
) -> list[dict[str, Any]]:
    if ano < 2000:
        raise ValueError("O ano informado não é válido.")

    if pagina < 1:
        raise ValueError("A página deve ser maior ou igual a 1.")

    resultado = client.get(
        "/emendas",
        params={
            "ano": ano,
            "pagina": pagina,
        },
    )

    if not isinstance(resultado, list):
        raise TypeError("O endpoint /emendas não retornou uma lista.")

    return resultado
