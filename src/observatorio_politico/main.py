from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from observatorio_politico.config import get_settings
from observatorio_politico.orchestration.emendas import run_emendas
from observatorio_politico.orchestration.gold_emendas import run_gold_emendas
from observatorio_politico.orchestration.historical_emendas import (
    run_historical_emendas,
)
from observatorio_politico.orchestration.historical_silver_emendas import (
    run_historical_silver_emendas,
)
from observatorio_politico.orchestration.orgaos_siafi import (
    run_orgaos_siafi,
)
from observatorio_politico.orchestration.silver_emendas import (
    run_silver_emendas,
)
from observatorio_politico.quality.reconciliation_emendas import (
    run_reconciliation_emendas,
)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format=("%(asctime)s %(levelname)s %(name)s - %(message)s"),
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
        help="Extrai órgãos SIAFI.",
    )

    emendas_parser = subparsers.add_parser(
        "emendas",
        help="Extrai emendas pela API.",
    )
    emendas_parser.add_argument(
        "--ano",
        type=int,
        required=True,
    )
    emendas_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
    )

    silver_parser = subparsers.add_parser(
        "silver-emendas",
        help="Transforma a Bronze da API em Silver.",
    )
    silver_parser.add_argument(
        "--ano",
        type=int,
        required=True,
    )
    reconciliation_parser = subparsers.add_parser(
        "reconciliar-emendas",
        help="Reconcilia os valores entre Silver e Gold.",
    )
    reconciliation_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
    )

    historical_parser = subparsers.add_parser(
        "historico-emendas",
        help="Importa os três CSVs históricos oficiais.",
    )
    historical_parser.add_argument(
        "--diretorio",
        type=Path,
        required=True,
        help="Diretório contendo os três CSVs oficiais.",
    )

    historical_silver_parser = subparsers.add_parser(
        "silver-historico-emendas",
        help="Transforma os arquivos históricos em Silver.",
    )
    historical_silver_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos que serão carregados. Exemplo: 2025 2026.",
    )

    gold_parser = subparsers.add_parser(
        "gold-emendas",
        help="Cria os indicadores Gold de emendas.",
    )
    gold_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos que serão carregados. Exemplo: 2025 2026.",
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

        elif args.command == "historico-emendas":
            run_historical_emendas(
                settings,
                diretorio=args.diretorio,
            )

        elif args.command == "silver-historico-emendas":
            run_historical_silver_emendas(
                settings,
                years=args.anos,
            )

        elif args.command == "gold-emendas":
            run_gold_emendas(
                years=args.anos,
            )
        elif args.command == "reconciliar-emendas":
            run_reconciliation_emendas(
                years=args.anos,
            )
        else:
            parser.error(f"Comando não implementado: {args.command}")

        logger.info("Pipeline concluído com sucesso.")
        return 0

    except Exception:
        logger.exception("Pipeline finalizado com erro.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
