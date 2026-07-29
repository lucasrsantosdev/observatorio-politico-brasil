from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path


GOLD_ROOT = Path(
    "data/gold/camara_deputados/"
    "proposicoes_votacoes/anos=2025_2026"
)

DIMENSIONS_ROOT = GOLD_ROOT / "dimensions"

MANIFEST_PATH = (
    DIMENSIONS_ROOT
    / "dimensions.manifest.json"
)

POWER_BI_ROOT = (
    Path("output/power_bi")
    / "proposicoes_votacoes"
)

CATALOG_PATH = (
    Path("output/power_bi")
    / "catalogo_dimensoes.csv"
)


def main() -> None:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Manifesto nao encontrado: {MANIFEST_PATH}"
        )

    manifest = json.loads(
        MANIFEST_PATH.read_text(
            encoding="utf-8"
        )
    )

    if not manifest.get("approved", False):
        raise RuntimeError(
            "Dimensoes legislativas nao aprovadas."
        )

    POWER_BI_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    existing_rows: list[dict[str, str]] = []

    if CATALOG_PATH.exists():
        with CATALOG_PATH.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as source:
            existing_rows = list(
                csv.DictReader(
                    source,
                    delimiter=";",
                )
            )

    existing_rows = [
        row
        for row in existing_rows
        if row.get("Dominio")
        != "proposicoes_votacoes"
    ]

    new_rows: list[dict[str, str]] = []

    for item in manifest["dimensions"]:
        source_name = item["dimension"]

        target_name = (
            "proposicoes_votacoes_"
            f"{source_name}"
        )

        source_parquet = (
            DIMENSIONS_ROOT
            / source_name
            / f"{source_name}.parquet"
        )

        source_csv = (
            DIMENSIONS_ROOT
            / source_name
            / f"{source_name}.csv"
        )

        target_parquet = (
            POWER_BI_ROOT
            / f"{target_name}.parquet"
        )

        target_csv = (
            POWER_BI_ROOT
            / f"{target_name}.csv"
        )

        if not source_parquet.exists():
            raise FileNotFoundError(
                f"Parquet ausente: {source_parquet}"
            )

        if not source_csv.exists():
            raise FileNotFoundError(
                f"CSV ausente: {source_csv}"
            )

        shutil.copy2(
            source_parquet,
            target_parquet,
        )

        shutil.copy2(
            source_csv,
            target_csv,
        )

        new_rows.append(
            {
                "Dominio": (
                    "proposicoes_votacoes"
                ),
                "Base": str(
                    DIMENSIONS_ROOT
                ),
                "Atual": source_name,
                "Novo": target_name,
            }
        )

        print(
            f"{target_name:<60} "
            f"PARQUET={target_parquet.exists()} "
            f"CSV={target_csv.exists()}"
        )

    fieldnames = [
        "Dominio",
        "Base",
        "Atual",
        "Novo",
    ]

    all_rows = (
        existing_rows
        + new_rows
    )

    with CATALOG_PATH.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as target:
        writer = csv.DictWriter(
            target,
            fieldnames=fieldnames,
            delimiter=";",
        )

        writer.writeheader()
        writer.writerows(all_rows)

    print()
    print(
        f"DIMENSOES_PUBLICADAS="
        f"{len(new_rows)}"
    )
    print(
        f"TOTAL_CATALOGO_DIMENSOES="
        f"{len(all_rows)}"
    )
    print(
        f"CATALOGO={CATALOG_PATH}"
    )


if __name__ == "__main__":
    main()
