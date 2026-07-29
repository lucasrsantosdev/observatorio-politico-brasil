from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from observatorio_politico.config import get_settings
from observatorio_politico.orchestration.contratos_bronze import (
    run_bronze_contratos,
)
from observatorio_politico.orchestration.contratos_silver import (
    run_silver_contratos,
)
from observatorio_politico.orchestration.dimensions_emendas import (
    run_dimensions_emendas,
)
from observatorio_politico.orchestration.dimensoes_contratos import (
    run_dimensions_contratos,
)
from observatorio_politico.orchestration.dimensoes_convenios import (
    run_dimensions_convenios,
)
from observatorio_politico.orchestration.dimensoes_gastos_deputados import (
    run_dimensions_gastos_deputados,
)
from observatorio_politico.orchestration.dimensoes_licitacoes import (
    run_dimensions_licitacoes,
)
from observatorio_politico.orchestration.emendas import run_emendas
from observatorio_politico.orchestration.gastos_deputados_bronze import (
    run_bronze_gastos_deputados,
)
from observatorio_politico.orchestration.gastos_deputados_silver import (
    run_silver_gastos_deputados,
)
from observatorio_politico.orchestration.gold_contratos import (
    run_gold_contratos,
)
from observatorio_politico.orchestration.gold_convenios import (
    run_gold_convenios,
)
from observatorio_politico.orchestration.gold_emendas import run_gold_emendas
from observatorio_politico.orchestration.gold_gastos_deputados import (
    run_gold_gastos_deputados,
)
from observatorio_politico.orchestration.gold_licitacoes import (
    run_gold_licitacoes,
)
from observatorio_politico.orchestration.historical_emendas import (
    run_historical_emendas,
)
from observatorio_politico.orchestration.historical_silver_emendas import (
    run_historical_silver_emendas,
)
from observatorio_politico.orchestration.licitacoes_bronze import (
    run_bronze_licitacoes,
)
from observatorio_politico.orchestration.licitacoes_silver import (
    run_silver_licitacoes,
)
from observatorio_politico.orchestration.orgaos_siafi import (
    run_orgaos_siafi,
)
from observatorio_politico.orchestration.silver_emendas import (
    run_silver_emendas,
)
from observatorio_politico.quality.contratos_silver import (
    run_quality_silver_contratos,
)
from observatorio_politico.quality.convenios_silver import (
    run_quality_silver_convenios,
)
from observatorio_politico.quality.gastos_deputados_silver import (
    run_quality_silver_gastos_deputados,
)
from observatorio_politico.quality.licitacoes_silver import (
    run_quality_silver_licitacoes,
)
from observatorio_politico.quality.reconciliation_contratos import (
    run_reconciliation_contratos,
)
from observatorio_politico.quality.reconciliation_convenios import (
    run_reconciliation_convenios,
)
from observatorio_politico.quality.reconciliation_emendas import (
    run_reconciliation_emendas,
)
from observatorio_politico.quality.reconciliation_gastos_deputados import (
    run_reconciliation_gastos_deputados,
)
from observatorio_politico.quality.reconciliation_licitacoes import (
    run_reconciliation_licitacoes,
)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format=("%(asctime)s %(levelname)s %(name)s - %(message)s"),
    )


from observatorio_politico.orchestration.gold_proposicoes_votacoes import (
    run_gold_proposicoes_votacoes,
)
from observatorio_politico.orchestration.proposicoes_votacoes_bronze import (
    run_bronze_proposicoes_votacoes,
)
from observatorio_politico.orchestration.proposicoes_votacoes_silver import (
    run_silver_proposicoes_votacoes,
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
    dimensions_parser = subparsers.add_parser(
        "dimensoes-emendas",
        help="Cria dimensões para o modelo Power BI.",
    )
    dimensions_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )
    contratos_bronze_parser = subparsers.add_parser(
        "bronze-contratos",
        help="Importa os arquivos mensais de contratos para a Bronze.",
    )
    contratos_bronze_parser.add_argument(
        "--diretorio",
        type=Path,
        required=True,
        help="Diretório contendo os CSVs extraídos de contratos.",
    )
    licitacoes_bronze_parser = subparsers.add_parser(
        "bronze-licitacoes",
        help="Importa os arquivos mensais de licitações para a Bronze.",
    )

    licitacoes_bronze_parser.add_argument(
        "--diretorio",
        type=Path,
        required=True,
        help="Diretório contendo os CSVs extraídos de licitações.",
    )
    licitacoes_silver_parser = subparsers.add_parser(
        "silver-licitacoes",
        help="Normaliza e consolida as licitações federais.",
    )

    licitacoes_silver_parser.add_argument(
        "--periodos",
        nargs="+",
        required=True,
        help="Períodos no formato AAAAMM.",
    )
    quality_licitacoes_parser = subparsers.add_parser(
        "validar-silver-licitacoes",
        help="Valida qualidade e integridade da Silver de licitações.",
    )

    quality_licitacoes_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    quality_licitacoes_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    gold_licitacoes_parser = subparsers.add_parser(
        "gold-licitacoes",
        help="Cria os rankings e indicadores Gold de licitações.",
    )

    gold_licitacoes_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    gold_licitacoes_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    contratos_silver_parser = subparsers.add_parser(
        "silver-contratos",
        help="Normaliza e consolida os contratos federais.",
    )

    contratos_silver_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    contratos_silver_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    quality_contratos_parser = subparsers.add_parser(
        "validar-silver-contratos",
        help=("Valida qualidade e integridade da Silver de contratos."),
    )

    quality_contratos_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    quality_contratos_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    gold_contratos_parser = subparsers.add_parser(
        "gold-contratos",
        help="Cria rankings e indicadores Gold de contratos.",
    )

    gold_contratos_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    gold_contratos_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    reconciliation_contratos_parser = subparsers.add_parser(
        "reconciliar-contratos",
        help="Reconcilia os dados Silver e Gold de contratos.",
    )

    reconciliation_contratos_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    reconciliation_contratos_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    dimensions_contratos_parser = subparsers.add_parser(
        "dimensoes-contratos",
        help="Cria dimensões de contratos para o Power BI.",
    )

    dimensions_contratos_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    dimensions_contratos_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )
    quality_convenios_parser = subparsers.add_parser(
        "validar-silver-convenios",
        help="Valida qualidade da Silver histórica de convênios.",
    )

    quality_convenios_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    gold_convenios_parser = subparsers.add_parser(
        "gold-convenios",
        help="Cria fatos e rankings Gold de convênios.",
    )

    gold_convenios_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )
    reconciliation_convenios_parser = subparsers.add_parser(
        "reconciliar-convenios",
        help="Reconcilia Silver e Gold de convênios.",
    )

    reconciliation_convenios_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    dimensions_convenios_parser = subparsers.add_parser(
        "dimensoes-convenios",
        help="Cria dimensões de convênios para o Power BI.",
    )

    dimensions_convenios_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )
    reconciliation_licitacoes_parser = subparsers.add_parser(
        "reconciliar-licitacoes",
        help="Reconcilia os dados Silver e Gold de licitações.",
    )

    reconciliation_licitacoes_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    reconciliation_licitacoes_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )

    dimensions_licitacoes_parser = subparsers.add_parser(
        "dimensoes-licitacoes",
        help="Cria dimensões de licitações para o Power BI.",
    )

    dimensions_licitacoes_parser.add_argument(
        "--periodo-inicial",
        required=True,
        help="Período inicial no formato AAAAMM.",
    )

    dimensions_licitacoes_parser.add_argument(
        "--periodo-final",
        required=True,
        help="Período final no formato AAAAMM.",
    )

    gastos_bronze_parser = subparsers.add_parser(
        "bronze-gastos-deputados",
        help="Baixa os arquivos anuais da cota parlamentar da Câmara.",
    )

    gastos_bronze_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    gastos_bronze_parser.add_argument(
        "--force",
        action="store_true",
        help="Baixa novamente os arquivos mesmo que já existam.",
    )

    gastos_silver_parser = subparsers.add_parser(
        "silver-gastos-deputados",
        help="Normaliza os gastos dos deputados na camada Silver.",
    )

    gastos_silver_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    quality_gastos_parser = subparsers.add_parser(
        "validar-silver-gastos-deputados",
        help="Valida a Silver de gastos dos deputados.",
    )

    quality_gastos_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    gold_gastos_parser = subparsers.add_parser(
        "gold-gastos-deputados",
        help="Cria fatos, rankings e indicadores Gold dos gastos.",
    )

    gold_gastos_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )
    reconciliation_gastos_parser = subparsers.add_parser(
        "reconciliar-gastos-deputados",
        help="Reconcilia Silver e Gold dos gastos dos deputados.",
    )
    reconciliation_gastos_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
    )

    dimensions_gastos_parser = subparsers.add_parser(
        "dimensoes-gastos-deputados",
        help="Cria dimensões dos gastos para o Power BI.",
    )
    dimensions_gastos_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
    )

    proposicoes_votacoes_bronze_parser = subparsers.add_parser(
        "bronze-proposicoes-votacoes",
        help="Baixa proposições e votações da Câmara.",
    )

    proposicoes_votacoes_bronze_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    proposicoes_votacoes_bronze_parser.add_argument(
        "--force",
        action="store_true",
        help="Baixa novamente arquivos já existentes.",
    )

    proposicoes_votacoes_silver_parser = subparsers.add_parser(
        "silver-proposicoes-votacoes",
        help="Normaliza proposi??es e vota??es da C?mara.",
    )

    proposicoes_votacoes_silver_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
    )

    gold_proposicoes_votacoes_parser = subparsers.add_parser(
        "gold-proposicoes-votacoes",
        help="Cria fatos e indicadores de proposi??es e vota??es.",
    )

    gold_proposicoes_votacoes_parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        required=True,
        help="Anos processados. Exemplo: 2025 2026.",
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

        elif args.command == "dimensoes-emendas":
            run_dimensions_emendas(
                years=args.anos,
            )

        elif args.command == "reconciliar-emendas":
            run_reconciliation_emendas(
                years=args.anos,
            )
        elif args.command == "bronze-contratos":
            run_bronze_contratos(
                landing_root=args.diretorio,
            )
        elif args.command == "bronze-licitacoes":
            run_bronze_licitacoes(
                landing_root=args.diretorio,
            )
        elif args.command == "silver-licitacoes":
            run_silver_licitacoes(
                periods=args.periodos,
            )
        elif args.command == "validar-silver-licitacoes":
            run_quality_silver_licitacoes(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "gold-licitacoes":
            run_gold_licitacoes(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "silver-contratos":
            run_silver_contratos(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "validar-silver-contratos":
            run_quality_silver_contratos(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "gold-contratos":
            run_gold_contratos(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "reconciliar-contratos":
            run_reconciliation_contratos(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "dimensoes-contratos":
            run_dimensions_contratos(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )
        elif args.command == "validar-silver-convenios":
            run_quality_silver_convenios(
                years=args.anos,
            )

        elif args.command == "gold-convenios":
            run_gold_convenios(
                years=args.anos,
            )
        elif args.command == "reconciliar-convenios":
            run_reconciliation_convenios(
                years=args.anos,
            )

        elif args.command == "dimensoes-convenios":
            run_dimensions_convenios(
                years=args.anos,
            )

        elif args.command == "reconciliar-licitacoes":
            run_reconciliation_licitacoes(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )

        elif args.command == "dimensoes-licitacoes":
            run_dimensions_licitacoes(
                first_period=args.periodo_inicial,
                last_period=args.periodo_final,
            )

        elif args.command == "bronze-gastos-deputados":
            run_bronze_gastos_deputados(
                years=args.anos,
                force=args.force,
            )

        elif args.command == "silver-gastos-deputados":
            run_silver_gastos_deputados(
                years=args.anos,
            )

        elif args.command == "validar-silver-gastos-deputados":
            run_quality_silver_gastos_deputados(
                years=args.anos,
            )

        elif args.command == "gold-gastos-deputados":
            run_gold_gastos_deputados(
                years=args.anos,
            )
        elif args.command == "reconciliar-gastos-deputados":
            run_reconciliation_gastos_deputados(
                years=args.anos,
            )

        elif args.command == "dimensoes-gastos-deputados":
            run_dimensions_gastos_deputados(
                years=args.anos,
            )

        elif args.command == "bronze-proposicoes-votacoes":
            run_bronze_proposicoes_votacoes(
                years=args.anos,
                force=args.force,
            )

        elif args.command == "silver-proposicoes-votacoes":
            run_silver_proposicoes_votacoes(
                years=args.anos,
            )

        elif args.command == "gold-proposicoes-votacoes":
            run_gold_proposicoes_votacoes(
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
