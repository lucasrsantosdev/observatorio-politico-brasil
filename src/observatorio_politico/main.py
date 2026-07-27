from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from observatorio_politico.config import get_settings
from observatorio_politico.orchestration.emendas import run_emendas
from observatorio_politico.orchestration.orgaos_siafi import (
    run_orgaos_siafi,
)
from observatorio_politico.orchestration.silver_emendas import (
    run_silver_emendas,
)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s - %(message)s"
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Observatório Político Brasil",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser(
        "orgaos-siafi",
        help="Extrai uma página de órgãos SIAFI.",
    )

    emendas_parser = subparsers.add_parser(
        "emendas",
        help="Extrai emendas parlamentares para a camada Bronze.",
    )
    emendas_parser.add_argument(
        "--ano",
        type=int,
        required=True,
        help="Ano das emendas.",
    )
    emendas_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limite opcional de páginas para testes.",
    )

    silver_emendas_parser = subparsers.add_parser(
        "silver-emendas",
        help="Transforma a última execução Bronze em Silver.",
    )
    silver_emendas_parser.add_argument(
        "--ano",
        type=int,
        required=True,
        help="Ano das emendas.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)

    logger = logging.getLogger(__name__)

    try:
        logger.info("Iniciando Observatório Político Brasil.")

        if args.command == "orgaos-siafi":
            run_orgaos_siafi(settings)

        elif args.command == "emendas":
            run_emendas(
                settings,
                ano=args.ano,
                max_pages=args.max_pages,
            )

        elif args.command == "silver-emendas":
            run_silver_emendas(
                settings,
                ano=args.ano,
            )

        else:
            parser.error(
                f"Comando não implementado: {args.command}"
            )

        logger.info("Pipeline concluído com sucesso.")
        return 0

    except Exception:
        logger.exception("Pipeline finalizado com erro.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
