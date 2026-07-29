from __future__ import annotations

import csv
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

GOLD_ROOT = Path("data/gold")
POWER_BI_ROOT = Path("output/power_bi")

EXCLUDED_PARTS = {
    "dimensions",
    "quality",
    "reconciliation",
}

ALLOWED_PREFIXES = (
    "fato_",
    "ranking_",
    "resumo_",
    "relacionamento_",
    "rel_",
    "agg_",
)


@dataclass(frozen=True)
class PublishedDataset:
    source_area: str
    domain: str
    dataset: str
    power_bi_table: str
    source_parquet: str
    target_parquet: str
    source_csv: str
    target_csv: str
    parquet_ok: bool
    csv_ok: bool


def identify_source_and_domain(
    path: Path,
) -> tuple[str, str]:
    parts = path.parts

    if "portal_transparencia" in parts:
        index = parts.index("portal_transparencia")

        if len(parts) <= index + 1:
            raise ValueError(f"Domínio não encontrado em {path}")

        return (
            "portal_transparencia",
            parts[index + 1],
        )

    if "camara_deputados" in parts:
        index = parts.index("camara_deputados")

        if len(parts) <= index + 1:
            raise ValueError(f"Domínio não encontrado em {path}")

        return (
            "camara_deputados",
            parts[index + 1],
        )

    raise ValueError(f"Origem Gold não reconhecida: {path}")


def find_datasets() -> list[Path]:
    datasets: list[Path] = []

    for parquet_path in GOLD_ROOT.rglob("*.parquet"):
        relative_parts = set(parquet_path.relative_to(GOLD_ROOT).parts)

        if relative_parts & EXCLUDED_PARTS:
            continue

        dataset = parquet_path.stem
        parent_dataset = parquet_path.parent.name

        if dataset != parent_dataset:
            continue

        if not dataset.startswith(ALLOWED_PREFIXES):
            continue

        datasets.append(parquet_path)

    return sorted(set(datasets))


def publish_dataset(
    parquet_path: Path,
) -> PublishedDataset:
    source_area, domain = identify_source_and_domain(parquet_path)

    dataset = parquet_path.stem

    if dataset.startswith(f"{domain}_"):
        power_bi_table = dataset
    else:
        power_bi_table = f"{domain}_{dataset}"

    csv_path = parquet_path.with_suffix(".csv")

    destination = POWER_BI_ROOT / domain

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    target_parquet = destination / f"{power_bi_table}.parquet"

    target_csv = destination / f"{power_bi_table}.csv"

    shutil.copy2(
        parquet_path,
        target_parquet,
    )

    csv_ok = False

    if csv_path.exists():
        shutil.copy2(
            csv_path,
            target_csv,
        )
        csv_ok = target_csv.exists()

    return PublishedDataset(
        source_area=source_area,
        domain=domain,
        dataset=dataset,
        power_bi_table=power_bi_table,
        source_parquet=str(parquet_path),
        target_parquet=str(target_parquet),
        source_csv=str(csv_path),
        target_csv=(str(target_csv) if csv_path.exists() else ""),
        parquet_ok=target_parquet.exists(),
        csv_ok=csv_ok,
    )


def write_catalog(
    published: list[PublishedDataset],
) -> Path:
    catalog_path = POWER_BI_ROOT / "catalogo_modelo.csv"

    catalog_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fieldnames = list(PublishedDataset.__dataclass_fields__)

    with catalog_path.open(
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

        for item in published:
            writer.writerow(asdict(item))

    return catalog_path


def main() -> None:
    datasets = find_datasets()

    if not datasets:
        raise RuntimeError(
            "Nenhum fato, ranking, resumo ou relacionamento Gold foi encontrado."
        )

    published = [publish_dataset(path) for path in datasets]

    catalog_path = write_catalog(published)

    print("=" * 100)
    print("PUBLICAÇÃO POWER BI")
    print("=" * 100)

    for item in published:
        print(
            f"{item.domain:<22} "
            f"{item.power_bi_table:<55} "
            f"PARQUET={item.parquet_ok} "
            f"CSV={item.csv_ok}"
        )

    print()
    print(f"TOTAL_DATASETS={len(published)}")
    print(f"TOTAL_PARQUETS={sum(item.parquet_ok for item in published)}")
    print(f"TOTAL_CSVS={sum(item.csv_ok for item in published)}")
    print(f"CATALOGO={catalog_path}")


if __name__ == "__main__":
    main()
