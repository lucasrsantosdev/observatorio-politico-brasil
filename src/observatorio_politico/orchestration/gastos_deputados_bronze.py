from __future__ import annotations

import hashlib
import json
import logging
import shutil
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BASE_URL = "https://www.camara.leg.br/cotas/Ano-{year}.csv.zip"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)

    return digest.hexdigest()


def _download(
    *,
    url: str,
    destination: Path,
) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": ("Observatorio-Politico-Brasil/1.0 (dados-publicos)")},
    )

    temporary = destination.with_suffix(destination.suffix + ".part")

    try:
        with (
            urllib.request.urlopen(
                request,
                timeout=180,
            ) as response,
            temporary.open("wb") as output,
        ):
            shutil.copyfileobj(
                response,
                output,
            )

        temporary.replace(destination)

    finally:
        temporary.unlink(
            missing_ok=True,
        )


def _extract_csv(
    *,
    zip_path: Path,
    destination: Path,
) -> list[Path]:
    extracted: list[Path] = []

    with zipfile.ZipFile(zip_path) as archive:
        csv_members = [
            member for member in archive.namelist() if member.lower().endswith(".csv")
        ]

        if not csv_members:
            raise ValueError(f"Nenhum CSV encontrado em {zip_path}")

        for member in csv_members:
            filename = Path(member).name
            output_path = destination / filename

        with (
            archive.open(member) as source,
            output_path.open("wb") as target,
        ):
            shutil.copyfileobj(
                source,
                target,
            )

            extracted.append(output_path)

    return extracted


def run_bronze_gastos_deputados(
    *,
    years: list[int],
    force: bool = False,
) -> Path:
    bronze_root = Path("data/bronze") / "camara_deputados" / "gastos_deputados"

    manifests: list[dict[str, object]] = []

    for year in sorted(set(years)):
        destination = bronze_root / f"ano={year}"

        destination.mkdir(
            parents=True,
            exist_ok=True,
        )

        zip_path = destination / f"Ano-{year}.csv.zip"

        url = BASE_URL.format(
            year=year,
        )

        if force or not zip_path.exists():
            logger.info(
                "Baixando despesas da Câmara: ano=%s url=%s",
                year,
                url,
            )

            _download(
                url=url,
                destination=zip_path,
            )
        else:
            logger.info(
                "Arquivo Bronze já existe: %s",
                zip_path,
            )

        extracted = _extract_csv(
            zip_path=zip_path,
            destination=destination,
        )

        files = []

        for path in extracted:
            files.append(
                {
                    "filename": path.name,
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )

        period_manifest = {
            "source": "camara_deputados",
            "subject": "gastos_deputados_ceap",
            "layer": "bronze",
            "year": year,
            "source_url": url,
            "downloaded_at_utc": datetime.now(UTC).isoformat(),
            "zip_file": str(zip_path),
            "zip_size_bytes": zip_path.stat().st_size,
            "zip_sha256": _sha256(zip_path),
            "extracted_files": files,
        }

        manifest_path = destination / "bronze.manifest.json"

        manifest_path.write_text(
            json.dumps(
                period_manifest,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        manifests.append(period_manifest)

        logger.info(
            "Bronze de gastos concluída: ano=%s arquivos=%s",
            year,
            len(extracted),
        )

    consolidated_root = bronze_root / "manifests"

    consolidated_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    years_label = "_".join(str(year) for year in sorted(set(years)))

    consolidated_path = consolidated_root / f"anos={years_label}.manifest.json"

    consolidated_path.write_text(
        json.dumps(
            {
                "source": "camara_deputados",
                "subject": "gastos_deputados_ceap",
                "layer": "bronze",
                "years": sorted(set(years)),
                "processed_at_utc": datetime.now(UTC).isoformat(),
                "periods": manifests,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return consolidated_path
