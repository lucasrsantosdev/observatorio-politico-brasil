from __future__ import annotations

import logging
import sys

from observatorio_politico.config import get_settings
from observatorio_politico.orchestration.orgaos_siafi import (
    run_orgaos_siafi,
)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s - %(message)s"
        ),
    )


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    logger = logging.getLogger(__name__)

    try:
        logger.info("Iniciando Observatório Político Brasil.")
        run_orgaos_siafi(settings)
        logger.info("Pipeline concluído com sucesso.")
        return 0
    except Exception:
        logger.exception("Pipeline finalizado com erro.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
