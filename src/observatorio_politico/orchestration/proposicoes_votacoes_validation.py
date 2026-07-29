from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


SILVER_TO_GOLD = {
    "proposicoes": "fato_proposicoes",
    "proposicoes_temas": "rel_proposicoes_temas",
    "proposicoes_autores": "rel_proposicoes_autores",
    "votacoes": "fato_votacoes",
    "votacoes_orientacoes": "rel_votacoes_orientacoes",
    "votacoes_votos": "fato_votos",
    "votacoes_objetos": "rel_votacoes_objetos",
    "votacoes_proposicoes": "rel_votacoes_proposicoes",
}


def _load_dataset(
    root: Path,
    dataset: str,
) -> pl.DataFrame:
    path = root / dataset / f"{dataset}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Dataset não encontrado: {path}")

    return pl.read_parquet(path)


def _null_count(
    dataframe: pl.DataFrame,
    column: str,
) -> int:
    if column not in dataframe.columns:
        return dataframe.height

    return int(dataframe.select(pl.col(column).null_count()).item())


def _duplicate_groups(
    dataframe: pl.DataFrame,
    columns: list[str],
) -> int:
    missing = [column for column in columns if column not in dataframe.columns]

    if missing:
        return dataframe.height

    return dataframe.group_by(columns).len().filter(pl.col("len") > 1).height


def _orphan_count(
    child: pl.DataFrame,
    parent: pl.DataFrame,
    *,
    child_key: str,
    parent_key: str,
) -> int:
    if child_key not in child.columns:
        return child.height

    if parent_key not in parent.columns:
        return child.height

    valid_child = child.filter(pl.col(child_key).is_not_null())

    parent_keys = (
        parent.select(pl.col(parent_key).alias(child_key))
        .filter(pl.col(child_key).is_not_null())
        .unique()
    )

    return valid_child.join(
        parent_keys,
        on=child_key,
        how="anti",
    ).height


def _check(
    *,
    name: str,
    value: int | bool,
    expected: int | bool,
    severity: str = "error",
    details: str = "",
) -> dict[str, Any]:
    passed = value == expected

    return {
        "name": name,
        "value": value,
        "expected": expected,
        "passed": passed,
        "severity": severity,
        "details": details,
    }


def run_quality_reconciliation_proposicoes_votacoes(
    *,
    years: list[int],
) -> tuple[Path, Path]:
    normalized_years = sorted(set(years))

    if not normalized_years:
        raise ValueError("Informe pelo menos um ano.")

    years_label = "_".join(str(year) for year in normalized_years)

    silver_root = (
        Path("data/silver")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    gold_root = (
        Path("data/gold")
        / "camara_deputados"
        / "proposicoes_votacoes"
        / f"anos={years_label}"
    )

    silver_manifest_path = silver_root / "silver.manifest.json"

    gold_manifest_path = gold_root / "gold.manifest.json"

    if not silver_manifest_path.exists():
        raise FileNotFoundError(f"Manifesto Silver ausente: {silver_manifest_path}")

    if not gold_manifest_path.exists():
        raise FileNotFoundError(f"Manifesto Gold ausente: {gold_manifest_path}")

    silver_manifest = json.loads(silver_manifest_path.read_text(encoding="utf-8"))

    gold_manifest = json.loads(gold_manifest_path.read_text(encoding="utf-8"))

    silver = {
        dataset: _load_dataset(
            silver_root,
            dataset,
        )
        for dataset in SILVER_TO_GOLD
    }

    gold = {
        dataset: _load_dataset(
            gold_root,
            dataset,
        )
        for dataset in {
            *SILVER_TO_GOLD.values(),
            "ranking_autores",
            "ranking_temas",
            "ranking_deputados_votos",
            "ranking_partidos_votos",
            "resumo_votacoes_mensal",
        }
    }

    fato_proposicoes = gold["fato_proposicoes"]

    fato_votacoes = gold["fato_votacoes"]

    fato_votos = gold["fato_votos"]

    rel_temas = gold["rel_proposicoes_temas"]

    rel_autores = gold["rel_proposicoes_autores"]

    rel_orientacoes = gold["rel_votacoes_orientacoes"]

    rel_objetos = gold["rel_votacoes_objetos"]

    rel_votacoes_proposicoes = gold["rel_votacoes_proposicoes"]

    quality_checks: list[dict[str, Any]] = []

    quality_checks.extend(
        [
            _check(
                name="silver_manifest_approved",
                value=bool(
                    silver_manifest.get(
                        "approved",
                        False,
                    )
                ),
                expected=True,
            ),
            _check(
                name="gold_uses_approved_silver",
                value=bool(
                    gold_manifest.get(
                        "silver_approved",
                        False,
                    )
                ),
                expected=True,
            ),
            _check(
                name="fato_proposicoes_id_null",
                value=_null_count(
                    fato_proposicoes,
                    "id_proposicao",
                ),
                expected=0,
            ),
            _check(
                name="fato_proposicoes_id_duplicate_groups",
                value=_duplicate_groups(
                    fato_proposicoes,
                    ["id_proposicao"],
                ),
                expected=0,
            ),
            _check(
                name="fato_votacoes_id_null",
                value=_null_count(
                    fato_votacoes,
                    "id_votacao",
                ),
                expected=0,
            ),
            _check(
                name="fato_votacoes_id_duplicate_groups",
                value=_duplicate_groups(
                    fato_votacoes,
                    ["id_votacao"],
                ),
                expected=0,
            ),
            _check(
                name="fato_votos_chave_null",
                value=_null_count(
                    fato_votos,
                    "chave_voto",
                ),
                expected=0,
            ),
            _check(
                name="fato_votos_chave_duplicate_groups",
                value=_duplicate_groups(
                    fato_votos,
                    ["chave_voto"],
                ),
                expected=0,
            ),
            _check(
                name="votos_sem_votacao",
                value=_orphan_count(
                    fato_votos,
                    fato_votacoes,
                    child_key="id_votacao",
                    parent_key="id_votacao",
                ),
                expected=0,
            ),
            _check(
                name="orientacoes_sem_votacao",
                value=_orphan_count(
                    rel_orientacoes,
                    fato_votacoes,
                    child_key="id_votacao",
                    parent_key="id_votacao",
                ),
                expected=0,
            ),
            _check(
                name="objetos_sem_votacao",
                value=_orphan_count(
                    rel_objetos,
                    fato_votacoes,
                    child_key="id_votacao",
                    parent_key="id_votacao",
                ),
                expected=0,
            ),
            _check(
                name="relacoes_proposicoes_sem_votacao",
                value=_orphan_count(
                    rel_votacoes_proposicoes,
                    fato_votacoes,
                    child_key="id_votacao",
                    parent_key="id_votacao",
                ),
                expected=0,
            ),
            _check(
                name="autores_sem_proposicao",
                value=_orphan_count(
                    rel_autores,
                    fato_proposicoes,
                    child_key="id_proposicao",
                    parent_key="id_proposicao",
                ),
                expected=0,
                severity="warning",
                details=(
                    "Foram preservadas 93 relacoes de autoria "
                    "referentes a 92 proposicoes ausentes no recorte. "
                    "Os registros incluem autorias institucionais, "
                    "como bancadas, liderancas e partidos politicos."
                ),
            ),
            _check(
                name="temas_sem_proposicao",
                value=_orphan_count(
                    rel_temas,
                    fato_proposicoes,
                    child_key="id_proposicao",
                    parent_key="id_proposicao",
                ),
                expected=0,
            ),
        ]
    )

    quality_error_checks = [
        check for check in quality_checks if check["severity"] == "error"
    ]

    quality_approved = all(bool(check["passed"]) for check in quality_error_checks)

    quality_manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "layer": "gold",
        "validation_type": "quality",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "check_count": len(quality_checks),
        "passed_check_count": sum(bool(check["passed"]) for check in quality_checks),
        "failed_check_count": sum(
            not bool(check["passed"]) for check in quality_checks
        ),
        "approved": quality_approved,
        "checks": quality_checks,
    }

    quality_path = gold_root / "quality.manifest.json"

    quality_path.write_text(
        json.dumps(
            quality_manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    reconciliation_checks: list[dict[str, Any]] = []

    for silver_dataset, gold_dataset in SILVER_TO_GOLD.items():
        source_count = silver[silver_dataset].height

        target_count = gold[gold_dataset].height

        reconciliation_checks.append(
            _check(
                name=(f"{silver_dataset}_record_count_preserved"),
                value=target_count,
                expected=source_count,
                details=(f"Silver={silver_dataset}; Gold={gold_dataset}"),
            )
        )

    ranking_deputados_total = int(
        gold["ranking_deputados_votos"].select(pl.col("quantidade_votos").sum()).item()
        or 0
    )

    ranking_partidos_total = int(
        gold["ranking_partidos_votos"].select(pl.col("quantidade_votos").sum()).item()
        or 0
    )

    resumo_votacoes_total = int(
        gold["resumo_votacoes_mensal"]
        .select(pl.col("quantidade_votacoes").sum())
        .item()
        or 0
    )

    reconciliation_checks.extend(
        [
            _check(
                name="ranking_deputados_total_votos",
                value=ranking_deputados_total,
                expected=fato_votos.height,
            ),
            _check(
                name="ranking_partidos_total_votos",
                value=ranking_partidos_total,
                expected=fato_votos.height,
            ),
            _check(
                name="resumo_mensal_total_votacoes",
                value=resumo_votacoes_total,
                expected=fato_votacoes.height,
            ),
            _check(
                name="gold_dataset_count",
                value=int(
                    gold_manifest.get(
                        "dataset_count",
                        0,
                    )
                ),
                expected=13,
            ),
        ]
    )

    reconciliation_approved = all(
        bool(check["passed"]) for check in reconciliation_checks
    )

    reconciliation_manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "validation_type": "reconciliation",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "quality_approved": quality_approved,
        "check_count": len(reconciliation_checks),
        "passed_check_count": sum(
            bool(check["passed"]) for check in reconciliation_checks
        ),
        "failed_check_count": sum(
            not bool(check["passed"]) for check in reconciliation_checks
        ),
        "approved": (quality_approved and reconciliation_approved),
        "checks": reconciliation_checks,
        "totals": {
            "proposicoes": (fato_proposicoes.height),
            "votacoes": fato_votacoes.height,
            "votos": fato_votos.height,
            "relacoes_temas": rel_temas.height,
            "relacoes_autores": (rel_autores.height),
            "orientacoes": (rel_orientacoes.height),
            "objetos": rel_objetos.height,
            "relacoes_votacao_proposicao": (rel_votacoes_proposicoes.height),
        },
    }

    reconciliation_path = gold_root / "reconciliation.manifest.json"

    reconciliation_path.write_text(
        json.dumps(
            reconciliation_manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Validação legislativa concluída: qualidade=%s reconciliação=%s",
        quality_approved,
        reconciliation_manifest["approved"],
    )

    print("=" * 100)
    print("QUALIDADE")
    print("=" * 100)

    for check in quality_checks:
        status = "OK" if check["passed"] else "FALHOU"

        print(
            f"{status:<7} "
            f"{check['name']:<55} "
            f"valor={check['value']} "
            f"esperado={check['expected']}"
        )

    print()
    print(f"QUALITY_APPROVED={quality_approved}")

    print()
    print("=" * 100)
    print("RECONCILIAÇÃO")
    print("=" * 100)

    for check in reconciliation_checks:
        status = "OK" if check["passed"] else "FALHOU"

        print(
            f"{status:<7} "
            f"{check['name']:<55} "
            f"valor={check['value']} "
            f"esperado={check['expected']}"
        )

    print()
    print(f"RECONCILIATION_APPROVED={reconciliation_manifest['approved']}")

    return (
        quality_path,
        reconciliation_path,
    )
