from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


SILVER_ROOT = Path("data/silver/senado_federal/anos=2025_2026")

GOLD_ROOT = Path("data/gold/senado_federal/anos=2025_2026")

AUDIT_ROOT = Path("output/auditoria/senado")


def _read_silver(
    dataset: str,
) -> pl.DataFrame:
    return pl.read_parquet(SILVER_ROOT / dataset / f"{dataset}.parquet")


def _read_gold(
    dataset: str,
) -> pl.DataFrame:
    return pl.read_parquet(GOLD_ROOT / dataset / f"{dataset}.parquet")


def _check(
    *,
    name: str,
    expected: Any,
    actual: Any,
    severity: str = "error",
    detail: str = "",
) -> dict[str, Any]:
    passed = expected == actual

    return {
        "check": name,
        "severity": severity,
        "passed": passed,
        "expected": expected,
        "actual": actual,
        "detail": detail,
    }


def _duplicate_groups(
    dataframe: pl.DataFrame,
    columns: list[str],
) -> int:
    return dataframe.group_by(columns).len().filter(pl.col("len") > 1).height


def run_senado_quality() -> Path:
    AUDIT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    silver_materias = _read_silver("materias")

    silver_votacoes = _read_silver("votacoes")

    silver_votos = _read_silver("votos")

    silver_ceaps = _read_silver("ceaps")

    fato_materias = _read_gold("fato_materias")

    fato_votacoes = _read_gold("fato_votacoes")

    fato_votos = _read_gold("fato_votos")

    fato_gastos = _read_gold("fato_gastos_senadores")

    ranking_senadores = _read_gold("ranking_senadores_gastos")

    ranking_fornecedores = _read_gold("ranking_fornecedores_ceaps")

    ranking_tipos = _read_gold("ranking_tipos_despesa_ceaps")

    resumo_gastos = _read_gold("resumo_gastos_mensal")

    resumo_atividade = _read_gold("resumo_atividade_mensal")

    checks: list[dict[str, Any]] = []

    checks.extend(
        [
            _check(
                name="materias_silver_gold",
                expected=silver_materias.height,
                actual=fato_materias.height,
            ),
            _check(
                name="votacoes_silver_gold",
                expected=silver_votacoes.height,
                actual=fato_votacoes.height,
            ),
            _check(
                name="votos_silver_gold",
                expected=silver_votos.height,
                actual=fato_votos.height,
            ),
            _check(
                name="ceaps_silver_gold",
                expected=silver_ceaps.height,
                actual=fato_gastos.height,
            ),
            _check(
                name="duplicidade_id_materia",
                expected=0,
                actual=_duplicate_groups(
                    fato_materias,
                    ["id_materia"],
                ),
            ),
            _check(
                name="duplicidade_id_votacao",
                expected=0,
                actual=_duplicate_groups(
                    fato_votacoes,
                    ["id_votacao"],
                ),
            ),
            _check(
                name="duplicidade_chave_voto",
                expected=0,
                actual=_duplicate_groups(
                    fato_votos,
                    ["chave_voto"],
                ),
            ),
            _check(
                name="duplicidade_id_gasto",
                expected=0,
                actual=_duplicate_groups(
                    fato_gastos,
                    ["id_gasto"],
                ),
            ),
        ]
    )

    votos_sem_votacao = fato_votos.join(
        fato_votacoes.select("id_votacao").unique(),
        left_on="codigo_sessao_votacao",
        right_on="id_votacao",
        how="anti",
    )

    votacoes_sem_materia = fato_votacoes.filter(
        pl.col("codigo_materia").is_not_null()
    ).join(
        fato_materias.select("id_materia").unique(),
        left_on="codigo_materia",
        right_on="id_materia",
        how="anti",
    )

    votos_sem_senador_atual = (
        fato_votos.select(
            [
                "codigo_parlamentar",
                "nome_parlamentar",
                "sigla_partido_parlamentar",
                "sigla_uf_parlamentar",
            ]
        )
        .unique()
        .filter(pl.col("codigo_parlamentar").is_not_null())
    )

    votos_sem_votacao.write_csv(
        AUDIT_ROOT / "votos_sem_votacao.csv",
        separator=";",
    )

    votacoes_sem_materia.write_csv(
        AUDIT_ROOT / "votacoes_sem_materia.csv",
        separator=";",
    )

    votos_sem_senador_atual.write_csv(
        AUDIT_ROOT / "parlamentares_identificados_nos_votos.csv",
        separator=";",
    )

    checks.extend(
        [
            _check(
                name="votos_sem_votacao",
                expected=0,
                actual=votos_sem_votacao.height,
            ),
            _check(
                name="votacoes_sem_materia",
                expected=0,
                actual=votacoes_sem_materia.height,
                severity="warning",
                detail=(
                    "Algumas votacoes podem apontar "
                    "para materias fora do recorte "
                    "retornado pela pesquisa basica."
                ),
            ),
        ]
    )

    total_silver_ceaps = float(
        silver_ceaps.select(pl.col("valor_reembolsado").fill_null(0.0).sum()).item()
    )

    total_gold_gastos = float(
        fato_gastos.select(pl.col("valor_gasto").fill_null(0.0).sum()).item()
    )

    total_ranking_senadores = float(
        ranking_senadores.select(pl.col("valor_total").fill_null(0.0).sum()).item()
    )

    total_ranking_fornecedores = float(
        ranking_fornecedores.select(pl.col("valor_total").fill_null(0.0).sum()).item()
    )

    total_ranking_tipos = float(
        ranking_tipos.select(pl.col("valor_total").fill_null(0.0).sum()).item()
    )

    total_resumo_gastos = float(
        resumo_gastos.select(pl.col("valor_total").fill_null(0.0).sum()).item()
    )

    checks.extend(
        [
            _check(
                name="valor_ceaps_silver_gold",
                expected=round(
                    total_silver_ceaps,
                    2,
                ),
                actual=round(
                    total_gold_gastos,
                    2,
                ),
            ),
            _check(
                name="valor_fato_ranking_senadores",
                expected=round(
                    total_gold_gastos,
                    2,
                ),
                actual=round(
                    total_ranking_senadores,
                    2,
                ),
            ),
            _check(
                name="valor_fato_ranking_fornecedores",
                expected=round(
                    total_gold_gastos,
                    2,
                ),
                actual=round(
                    total_ranking_fornecedores,
                    2,
                ),
            ),
            _check(
                name="valor_fato_ranking_tipos",
                expected=round(
                    total_gold_gastos,
                    2,
                ),
                actual=round(
                    total_ranking_tipos,
                    2,
                ),
            ),
            _check(
                name="valor_fato_resumo_mensal",
                expected=round(
                    total_gold_gastos,
                    2,
                ),
                actual=round(
                    total_resumo_gastos,
                    2,
                ),
            ),
            _check(
                name="quantidade_votacoes_resumo",
                expected=fato_votacoes.height,
                actual=int(
                    resumo_atividade.select(pl.col("quantidade_votacoes").sum()).item()
                ),
            ),
        ]
    )

    error_checks = [item for item in checks if item["severity"] == "error"]

    warning_checks = [item for item in checks if item["severity"] == "warning"]

    approved = all(bool(item["passed"]) for item in error_checks)

    report = {
        "source": "senado_federal",
        "years": [2025, 2026],
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": len(checks),
        "error_check_count": len(error_checks),
        "warning_check_count": len(warning_checks),
        "passed_count": sum(bool(item["passed"]) for item in checks),
        "failed_count": sum(not bool(item["passed"]) for item in checks),
        "approved": approved,
        "financial_reconciliation": {
            "silver_ceaps": round(
                total_silver_ceaps,
                2,
            ),
            "gold_fato_gastos": round(
                total_gold_gastos,
                2,
            ),
            "ranking_senadores": round(
                total_ranking_senadores,
                2,
            ),
            "ranking_fornecedores": round(
                total_ranking_fornecedores,
                2,
            ),
            "ranking_tipos_despesa": round(
                total_ranking_tipos,
                2,
            ),
            "resumo_gastos_mensal": round(
                total_resumo_gastos,
                2,
            ),
        },
        "checks": checks,
        "audit_files": {
            "votos_sem_votacao": str(AUDIT_ROOT / "votos_sem_votacao.csv"),
            "votacoes_sem_materia": str(AUDIT_ROOT / "votacoes_sem_materia.csv"),
            "parlamentares_votos": str(
                AUDIT_ROOT / "parlamentares_identificados_nos_votos.csv"
            ),
        },
    }

    report_path = AUDIT_ROOT / "senado_quality.json"

    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    checks_dataframe = pl.DataFrame(
        checks,
        strict=False,
    )

    checks_dataframe.write_csv(
        AUDIT_ROOT / "senado_quality_checks.csv",
        separator=";",
    )

    print("=" * 100)
    print("QUALIDADE E RECONCILIACAO DO SENADO")
    print("=" * 100)

    for item in checks:
        status = (
            "PASS"
            if item["passed"]
            else ("WARN" if item["severity"] == "warning" else "FAIL")
        )

        print(
            f"{status:<5} "
            f"{item['check']:<40} "
            f"esperado={item['expected']} "
            f"atual={item['actual']}"
        )

    print()
    print(f"CHECKS={len(checks)}")

    print(f"APROVADOS={report['passed_count']}")

    print(f"REPROVADOS={report['failed_count']}")

    print(f"QUALITY_APPROVED={approved}")

    print(f"RELATORIO={report_path}")

    logger.info(
        "Qualidade Senado concluida: checks=%s aprovado=%s",
        len(checks),
        approved,
    )

    return report_path
