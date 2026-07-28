from __future__ import annotations

import logging
from typing import Any, Self

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class PortalTransparenciaError(RuntimeError):
    """Falha durante uma consulta ao Portal da Transparência."""


class PortalTransparenciaClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout_seconds: float = 60,
    ) -> None:
        if not api_key.strip():
            raise ValueError("PORTAL_TRANSPARENCIA_API_KEY não foi configurada.")

        self._client = httpx.Client(
            base_url=f"{base_url.rstrip('/')}/",
            timeout=httpx.Timeout(timeout_seconds),
            headers={
                "chave-api-dados": api_key.strip(),
                "Accept": "application/json",
                "User-Agent": "observatorio-politico-brasil/0.1.0",
            },
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    @retry(
        retry=retry_if_exception_type(
            (
                httpx.TimeoutException,
                httpx.NetworkError,
                PortalTransparenciaError,
            )
        ),
        wait=wait_exponential(
            multiplier=1,
            min=2,
            max=30,
        ),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        endpoint_normalizado = endpoint.lstrip("/")

        logger.info(
            "Consultando endpoint=%s params=%s",
            endpoint_normalizado,
            params,
        )

        response = self._client.get(
            endpoint_normalizado,
            params=params,
        )

        logger.info(
            "Resposta recebida endpoint=%s status=%s",
            endpoint_normalizado,
            response.status_code,
        )

        if response.status_code == 429:
            raise PortalTransparenciaError("Limite de requisições atingido: HTTP 429.")

        if response.status_code >= 500:
            raise PortalTransparenciaError(
                f"Erro temporário do Portal: HTTP {response.status_code}."
            )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise PortalTransparenciaError(
                f"Consulta recusada: HTTP {response.status_code}. "
                f"Endpoint: {endpoint_normalizado}. "
                f"Resposta: {response.text[:1000]}"
            ) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise PortalTransparenciaError(
                "A API respondeu com conteúdo que não é JSON."
            ) from exc
