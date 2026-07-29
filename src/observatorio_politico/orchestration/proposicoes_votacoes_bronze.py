from __future__ import annotations

import hashlib
import json
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

BASE_URL = "https://dadosabertos.camara.leg.br/arquivos"

DATASETS = {
    "proposicoes": {
        "remote_name": "proposicoes",
        "file_prefix": "proposicoes",
    },
    "proposicoes_temas": {
        "remote_name": "proposicoesTemas",
        "file_prefix": "proposicoesTemas",
    },
    "proposicoes_autores": {
        "remote_name": "proposicoesAutores",
        "file_prefix": "proposicoesAutores",
    },
    "votacoes": {
        "remote_name": "votacoes",
        "file_prefix": "votacoes",
    },
    "votacoes_orientacoes": {
        "remote_name": "votacoesOrientacoes",
        "file_prefix": "votacoesOrientacoes",
    },
    "votacoes_votos": {
        "remote_name": "votacoesVotos",
        "file_prefix": "votacoesVotos",
    },
    "votacoes_objetos": {
        "remote_name": "votacoesObjetos",
        "file_prefix": "votacoesObjetos",
    },
    "votacoes_proposicoes": {
        "remote_name": "votacoesProposicoes",
        "file_prefix": "votacoesProposicoes",
    },
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as source:
        for chunk in iter(
            lambda: source.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def _download(
    *,
    url: str,
    destination: Path,
    force: bool,
) -> bool:
    if destination.exists() and not force:
        logger.info(
            "Arquivo Bronze já existe: %s",
            destination,
        )
        return False

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = destination.with_suffix(destination.suffix + ".part")

    request = Request(
        url,
        headers={
            "User-Agent": ("Observatorio-Politico-Brasil/1.0 (dados-publicos)"),
            "Accept": "text/csv,*/*",
        },
    )

    try:
        with (
            urlopen(
                request,
                timeout=180,
            ) as response,
            temporary_path.open("wb") as target,
        ):
            shutil.copyfileobj(
                response,
                target,
            )

        temporary_path.replace(destination)

    except (HTTPError, URLError, TimeoutError) as error:
        temporary_path.unlink(missing_ok=True)

        raise RuntimeError(
            f"Falha ao baixar arquivo da Câmara: url={url} erro={error}"
        ) from error

    return True


def run_bronze_proposicoes_votacoes(
    *,
    years: list[int],
    force: bool = False,
) -> Path:
    normalized_years = sorted(set(years))

    if not normalized_years:
        raise ValueError("Informe pelo menos um ano.")

    bronze_root = Path("data/bronze") / "camara_deputados" / "proposicoes_votacoes"

    files: list[dict[str, object]] = []

    for dataset, configuration in DATASETS.items():
        remote_name = configuration["remote_name"]
        file_prefix = configuration["file_prefix"]

        for year in normalized_years:
            filename = f"{file_prefix}-{year}.csv"

            url = f"{BASE_URL}/{remote_name}/csv/{filename}"

            destination = bronze_root / f"dataset={dataset}" / f"ano={year}" / filename

            logger.info(
                "Baixando dados legislativos: dataset=%s ano=%s url=%s",
                dataset,
                year,
                url,
            )

            downloaded = _download(
                url=url,
                destination=destination,
                force=force,
            )

            file_size = destination.stat().st_size

            if file_size == 0:
                raise RuntimeError(f"Arquivo vazio: {destination}")

            files.append(
                {
                    "dataset": dataset,
                    "year": year,
                    "url": url,
                    "file": str(destination),
                    "file_size_bytes": file_size,
                    "sha256": _sha256(destination),
                    "downloaded": downloaded,
                }
            )

            logger.info(
                "Arquivo Bronze concluído: dataset=%s ano=%s bytes=%s",
                dataset,
                year,
                file_size,
            )

    manifest = {
        "source": "camara_deputados",
        "subject": "proposicoes_votacoes",
        "layer": "bronze",
        "years": normalized_years,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "dataset_count": len(DATASETS),
        "file_count": len(files),
        "files": files,
        "methodology_notes": [
            ("Proposições, temas e autores são organizados pelo ano de apresentação."),
            (
                "Votações, orientações, votos, objetos "
                "e proposições afetadas são organizados "
                "pelo ano de ocorrência da votação."
            ),
            (
                "Objetos possíveis e proposições afetadas "
                "são preservados em conjuntos separados."
            ),
            (
                "Os arquivos originais são armazenados "
                "sem transformação na camada Bronze."
            ),
        ],
    }

    manifest_path = bronze_root / "bronze.manifest.json"

    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Bronze de proposições e votações concluída: arquivos=%s manifesto=%s",
        len(files),
        manifest_path,
    )

    return manifest_path
