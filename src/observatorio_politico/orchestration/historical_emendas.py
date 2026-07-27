from __future__ import annotations

import logging
from pathlib import Path

from observatorio_politico.config import Settings
from observatorio_politico.loaders.historical_file import (
    import_historical_file,
)

logger = logging.getLogger(__name__)


HISTORICAL_FILES = {
    "EmendasParlamentares.csv": "emendas",
    "EmendasParlamentares_Convenios.csv": "emendas_convenios",
    "EmendasParlamentares_PorFavorecido.csv": "emendas_favorecidos",
}


def run_historical_emendas(
    settings: Settings,
    *,
    diretorio: Path,
) -> list[Path]:
    if not diretorio.exists():
        raise FileNotFoundError(f"Diretório não encontrado: {diretorio}")

    manifestos: list[Path] = []

    for filename, entity in HISTORICAL_FILES.items():
        arquivo = diretorio / filename

        if not arquivo.exists():
            raise FileNotFoundError(f"Arquivo obrigatório não encontrado: {arquivo}")

        logger.info(
            "Importando histórico: entidade=%s arquivo=%s",
            entity,
            arquivo,
        )

        manifesto = import_historical_file(
            source_file=arquivo,
            bronze_root=settings.bronze_path,
            entity=entity,
            ano=None,
        )
        manifestos.append(manifesto)

    logger.info(
        "Pacote histórico concluído: arquivos=%s",
        len(manifestos),
    )

    return manifestos
