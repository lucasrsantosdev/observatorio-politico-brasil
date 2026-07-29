from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

GOLD_ROOT = Path("data/gold/senado_federal/anos=2025_2026")

DIMENSIONS_ROOT = GOLD_ROOT / "dimensoes"

OUTPUT_ROOT = Path("output/power_bi/senado_federal")

AUDIT_ROOT = Path("output/auditoria/senado")


GOLD_DATASETS = (
    "fato_materias",
    "fato_votacoes",
    "fato_votos",
    "fato_gastos_senadores",
    "dim_senadores_base",
    "dim_empresas_contratadas_base",
    "ranking_senadores_gastos",
    "ranking_fornecedores_ceaps",
    "ranking_tipos_despesa_ceaps",
    "ranking_senadores_votos",
    "ranking_partidos_votos",
    "resumo_gastos_mensal",
    "resumo_atividade_mensal",
)

DIMENSION_DATASETS = (
    "dim_senador",
    "dim_materia",
    "dim_partido",
    "dim_uf",
    "dim_tipo_materia",
    "dim_tipo_voto",
    "dim_resultado_votacao",
    "dim_tipo_despesa",
    "dim_fornecedor",
    "dim_tempo",
)


def _publish_file(
    *,
    source: Path,
    target: Path,
) -> dict[str, Any]:
    if not source.exists():
        raise FileNotFoundError(f"Arquivo de origem nao encontrado: {source}")

    target.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.copy2(
        source,
        target,
    )

    return {
        "source": str(source),
        "target": str(target),
        "size_bytes": target.stat().st_size,
        "exists": target.exists(),
    }


def _publish_dataset(
    *,
    dataset: str,
    source_root: Path,
    category: str,
) -> dict[str, Any]:
    dataset_root = source_root / dataset

    parquet_source = dataset_root / f"{dataset}.parquet"

    csv_source = dataset_root / f"{dataset}.csv"

    parquet_target = OUTPUT_ROOT / f"senado_{dataset}.parquet"

    csv_target = OUTPUT_ROOT / f"senado_{dataset}.csv"

    parquet = _publish_file(
        source=parquet_source,
        target=parquet_target,
    )

    csv = _publish_file(
        source=csv_source,
        target=csv_target,
    )

    return {
        "domain": "senado_federal",
        "category": category,
        "dataset": dataset,
        "power_bi_table": (f"senado_{dataset}"),
        "parquet": parquet,
        "csv": csv,
        "approved": (
            parquet["exists"]
            and csv["exists"]
            and parquet["size_bytes"] > 0
            and csv["size_bytes"] > 0
        ),
    }


def main() -> None:
    quality_path = AUDIT_ROOT / "senado_quality.json"

    dimensions_manifest_path = DIMENSIONS_ROOT / "dimensions.manifest.json"

    gold_manifest_path = GOLD_ROOT / "gold.manifest.json"

    quality = json.loads(quality_path.read_text(encoding="utf-8"))

    dimensions_manifest = json.loads(
        dimensions_manifest_path.read_text(encoding="utf-8")
    )

    gold_manifest = json.loads(gold_manifest_path.read_text(encoding="utf-8"))

    if not quality.get("approved"):
        raise RuntimeError("Qualidade do Senado nao aprovada.")

    if not gold_manifest.get("approved"):
        raise RuntimeError("Gold do Senado nao aprovada.")

    if not dimensions_manifest.get("approved"):
        raise RuntimeError("Dimensoes do Senado nao aprovadas.")

    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    outputs = [
        _publish_dataset(
            dataset=dataset,
            source_root=GOLD_ROOT,
            category="gold",
        )
        for dataset in GOLD_DATASETS
    ]

    outputs.extend(
        _publish_dataset(
            dataset=dataset,
            source_root=DIMENSIONS_ROOT,
            category="dimension",
        )
        for dataset in DIMENSION_DATASETS
    )

    approved = all(item["approved"] for item in outputs)

    manifest = {
        "domain": "senado_federal",
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "gold_approved": True,
        "quality_approved": True,
        "dimensions_approved": True,
        "gold_dataset_count": len(GOLD_DATASETS),
        "dimension_count": len(DIMENSION_DATASETS),
        "dataset_count": len(outputs),
        "file_count": len(outputs) * 2,
        "approved": approved,
        "datasets": outputs,
    }

    manifest_path = OUTPUT_ROOT / "senado_power_bi.manifest.json"

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    catalog_path = OUTPUT_ROOT / "catalogo_senado_power_bi.csv"

    lines = [
        (
            "domain;category;dataset;"
            "power_bi_table;parquet_file;"
            "csv_file;parquet_ok;csv_ok"
        )
    ]

    for item in outputs:
        lines.append(
            ";".join(
                [
                    item["domain"],
                    item["category"],
                    item["dataset"],
                    item["power_bi_table"],
                    item["parquet"]["target"],
                    item["csv"]["target"],
                    str(item["parquet"]["exists"]),
                    str(item["csv"]["exists"]),
                ]
            )
        )

    catalog_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    print("=" * 100)
    print("PUBLICACAO POWER BI - SENADO FEDERAL")
    print("=" * 100)

    for item in outputs:
        status = "OK" if item["approved"] else "FALHOU"

        print(f"{status:<7} {item['category']:<10} {item['power_bi_table']}")

    print()
    print(f"GOLD_DATASETS={len(GOLD_DATASETS)}")
    print(f"DIMENSIONS={len(DIMENSION_DATASETS)}")
    print(f"DATASETS={len(outputs)}")
    print(f"FILES={len(outputs) * 2}")
    print(f"POWER_BI_APPROVED={approved}")
    print(f"MANIFEST={manifest_path}")
    print(f"CATALOG={catalog_path}")


if __name__ == "__main__":
    main()
