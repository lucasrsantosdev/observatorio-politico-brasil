from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


BRONZE_ROOT = Path("data/bronze/senado_federal")


def _snake_case(value: str) -> str:
    normalized = unicodedata.normalize(
        "NFKD",
        value,
    )

    ascii_value = "".join(
        character for character in normalized if not unicodedata.combining(character)
    )

    ascii_value = re.sub(
        r"([a-z0-9])([A-Z])",
        r"\1_\2",
        ascii_value,
    )

    ascii_value = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        ascii_value,
    )

    return ascii_value.strip("_").lower()


def _flatten(
    value: dict[str, Any],
    *,
    prefix: str = "",
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, child in value.items():
        normalized_key = _snake_case(key)

        column = f"{prefix}_{normalized_key}" if prefix else normalized_key

        if isinstance(child, dict):
            result.update(
                _flatten(
                    child,
                    prefix=column,
                )
            )

        elif isinstance(child, list):
            result[column] = json.dumps(
                child,
                ensure_ascii=False,
            )

        else:
            result[column] = child

    return result


def _hash_record(
    record: dict[str, Any],
) -> str:
    serialized = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _read_json(
    path: Path,
) -> Any:
    return json.loads(path.read_bytes().decode("utf-8-sig"))


def _read_csv(
    path: Path,
) -> list[dict[str, str]]:
    text = path.read_bytes().decode("utf-8-sig")

    reader = csv.DictReader(
        text.splitlines(),
        delimiter=";",
    )

    return [
        {
            _snake_case(str(key or "")): str(value or "").strip()
            for key, value in row.items()
        }
        for row in reader
    ]


def _stringify_complex_values(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []

    for record in records:
        normalized: dict[str, Any] = {}

        for key, value in record.items():
            if isinstance(
                value,
                (dict, list),
            ):
                normalized[key] = json.dumps(
                    value,
                    ensure_ascii=False,
                )
            else:
                normalized[key] = value

        output.append(normalized)

    return output


def _to_dataframe(
    records: list[dict[str, Any]],
) -> pl.DataFrame:
    if not records:
        return pl.DataFrame()

    return pl.DataFrame(
        _stringify_complex_values(records),
        infer_schema_length=None,
        strict=False,
    )


def _normalize_strings(
    dataframe: pl.DataFrame,
) -> pl.DataFrame:
    string_columns = [
        column for column, dtype in dataframe.schema.items() if dtype == pl.String
    ]

    if not string_columns:
        return dataframe

    return dataframe.with_columns(
        [
            pl.col(column).str.strip_chars().replace("", None).alias(column)
            for column in string_columns
        ]
    )


def _cast_existing(
    dataframe: pl.DataFrame,
    *,
    integer_columns: tuple[str, ...] = (),
    float_columns: tuple[str, ...] = (),
    date_columns: tuple[str, ...] = (),
) -> pl.DataFrame:
    expressions: list[pl.Expr] = []

    for column in integer_columns:
        if column in dataframe.columns:
            expressions.append(
                pl.col(column)
                .cast(
                    pl.String,
                    strict=False,
                )
                .str.replace_all(
                    r"[^0-9-]",
                    "",
                )
                .cast(
                    pl.Int64,
                    strict=False,
                )
                .alias(column)
            )

    for column in float_columns:
        if column in dataframe.columns:
            expressions.append(
                pl.col(column)
                .cast(
                    pl.String,
                    strict=False,
                )
                .str.replace_all(r"\.", "")
                .str.replace_all(",", ".")
                .cast(
                    pl.Float64,
                    strict=False,
                )
                .alias(column)
            )

    for column in date_columns:
        if column in dataframe.columns:
            expressions.append(
                pl.col(column)
                .cast(
                    pl.String,
                    strict=False,
                )
                .str.to_date(
                    strict=False,
                )
                .alias(column)
            )

    if not expressions:
        return dataframe

    return dataframe.with_columns(expressions)


def _with_audit_columns(
    dataframe: pl.DataFrame,
    *,
    dataset: str,
) -> pl.DataFrame:
    data_columns = list(dataframe.columns)

    dataframe = dataframe.with_row_index(
        name="linha_origem",
        offset=1,
    )

    dataframe = dataframe.with_columns(
        pl.lit(dataset).alias("dataset"),
        pl.concat_str(
            [
                pl.lit(dataset),
                pl.col("linha_origem").cast(pl.String),
            ],
            separator="|",
        ).alias("chave_registro"),
    )

    hash_expressions = [
        pl.col(column)
        .cast(
            pl.String,
            strict=False,
        )
        .fill_null("")
        for column in data_columns
    ]

    dataframe = dataframe.with_columns(
        pl.concat_str(
            hash_expressions,
            separator="|",
        )
        .map_elements(
            lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest(),
            return_dtype=pl.String,
        )
        .alias("hash_registro")
    )

    return dataframe.select(
        "chave_registro",
        "hash_registro",
        "dataset",
        *data_columns,
        "linha_origem",
    )


def _build_senadores() -> pl.DataFrame:
    path = BRONZE_ROOT / "dataset=senadores" / "senadores_atuais.json"

    content = _read_json(path)

    records = content["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]

    flattened = [_flatten(record) for record in records]

    dataframe = _to_dataframe(flattened)

    dataframe = _normalize_strings(dataframe)

    dataframe = _cast_existing(
        dataframe,
        integer_columns=(
            "identificacao_parlamentar_codigo_parlamentar",
            "identificacao_parlamentar_codigo_publico_na_leg_atual",
            "mandato_codigo_mandato",
            "mandato_numero_legislatura",
        ),
        date_columns=(
            "mandato_primeira_legislatura_data_inicio",
            "mandato_primeira_legislatura_data_fim",
            "mandato_segunda_legislatura_data_inicio",
            "mandato_segunda_legislatura_data_fim",
        ),
    )

    return _with_audit_columns(
        dataframe,
        dataset="senadores",
    )


def _build_materias() -> pl.DataFrame:
    path = BRONZE_ROOT / "dataset=materias" / "materias.json"

    content = _read_json(path)

    records = content["PesquisaBasicaMateria"]["Materias"]["Materia"]

    dataframe = _to_dataframe([_flatten(record) for record in records])

    dataframe = _normalize_strings(dataframe)

    dataframe = _cast_existing(
        dataframe,
        integer_columns=(
            "codigo",
            "numero",
            "ano",
        ),
        date_columns=("data",),
    )

    return _with_audit_columns(
        dataframe,
        dataset="materias",
    )


def _build_votacoes_e_votos() -> tuple[pl.DataFrame, pl.DataFrame]:
    path = BRONZE_ROOT / "dataset=votacoes" / "votacoes.json"

    content = _read_json(path)

    votacoes_records: list[dict[str, Any]] = []

    votos_records: list[dict[str, Any]] = []

    for votacao in content:
        votacao_record = {
            key: value for key, value in votacao.items() if key != "votos"
        }

        votacao_flat = _flatten(votacao_record)

        votacoes_records.append(votacao_flat)

        codigo_votacao = votacao.get("codigoSessaoVotacao")

        codigo_materia = votacao.get("codigoMateria")

        data_sessao = votacao.get("dataSessao")

        for voto in votacao.get("votos") or []:
            voto_flat = _flatten(voto)

            voto_flat["codigo_sessao_votacao"] = codigo_votacao

            voto_flat["codigo_materia"] = codigo_materia

            voto_flat["data_sessao"] = data_sessao

            votos_records.append(voto_flat)

    votacoes = _to_dataframe(votacoes_records)

    votacoes = _normalize_strings(votacoes)

    votacoes = _cast_existing(
        votacoes,
        integer_columns=(
            "ano",
            "codigo_materia",
            "codigo_sessao",
            "codigo_sessao_legislativa",
            "codigo_sessao_votacao",
            "numero_sessao",
            "sequencial_sessao",
            "sequencial_votacao",
            "total_votos_abstencao",
            "total_votos_nao",
            "total_votos_sim",
        ),
        date_columns=(
            "data_apresentacao",
            "data_sessao",
            "informe_legislativo_data",
        ),
    )

    votos = _to_dataframe(votos_records)

    votos = _normalize_strings(votos)

    votos = _cast_existing(
        votos,
        integer_columns=(
            "codigo_parlamentar",
            "codigo_sessao_votacao",
            "codigo_materia",
        ),
        date_columns=("data_sessao",),
    )

    return (
        _with_audit_columns(
            votacoes,
            dataset="votacoes",
        ),
        _with_audit_columns(
            votos,
            dataset="votos",
        ),
    )


def _build_ceaps() -> pl.DataFrame:
    paths = [
        (BRONZE_ROOT / "dataset=ceaps" / "ano=2025" / "ceaps_2025.csv"),
        (BRONZE_ROOT / "dataset=ceaps" / "ano=2026" / "ceaps_2026.csv"),
    ]

    records: list[dict[str, Any]] = []

    for path in paths:
        records.extend(_read_csv(path))

    dataframe = _to_dataframe(records)

    dataframe = _normalize_strings(dataframe)

    dataframe = _cast_existing(
        dataframe,
        integer_columns=(
            "id",
            "ano",
            "mes",
            "cod_senador",
        ),
        float_columns=("valor_reembolsado",),
        date_columns=("data",),
    )

    return _with_audit_columns(
        dataframe,
        dataset="ceaps",
    )


def _build_empresas() -> pl.DataFrame:
    path = BRONZE_ROOT / "dataset=empresas_contratadas" / "empresas_contratadas.csv"

    dataframe = _to_dataframe(_read_csv(path))

    dataframe = _normalize_strings(dataframe)

    dataframe = _cast_existing(
        dataframe,
        integer_columns=("codigo",),
    )

    return _with_audit_columns(
        dataframe,
        dataset="empresas_contratadas",
    )


def _write_dataset(
    dataframe: pl.DataFrame,
    *,
    root: Path,
    dataset: str,
) -> dict[str, Any]:
    destination = root / dataset

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    parquet_path = destination / f"{dataset}.parquet"

    csv_path = destination / f"{dataset}.csv"

    dataframe.write_parquet(
        parquet_path,
        compression="zstd",
        statistics=True,
    )

    dataframe.write_csv(
        csv_path,
        separator=";",
    )

    duplicate_keys = (
        dataframe.group_by("chave_registro").len().filter(pl.col("len") > 1).height
    )

    return {
        "dataset": dataset,
        "record_count": dataframe.height,
        "column_count": dataframe.width,
        "distinct_record_keys": (dataframe["chave_registro"].n_unique()),
        "duplicate_record_key_groups": (duplicate_keys),
        "columns": dataframe.columns,
        "parquet_file": str(parquet_path),
        "csv_file": str(csv_path),
    }


def run_senado_silver() -> Path:
    bronze_manifest_path = BRONZE_ROOT / "bronze.manifest.json"

    bronze_manifest = json.loads(bronze_manifest_path.read_text(encoding="utf-8"))

    if not bronze_manifest.get(
        "approved",
        False,
    ):
        raise RuntimeError("Bronze do Senado nao aprovada.")

    silver_root = Path("data/silver") / "senado_federal" / "anos=2025_2026"

    senadores = _build_senadores()
    materias = _build_materias()

    votacoes, votos = _build_votacoes_e_votos()

    ceaps = _build_ceaps()
    empresas = _build_empresas()

    datasets = {
        "senadores": senadores,
        "materias": materias,
        "votacoes": votacoes,
        "votos": votos,
        "ceaps": ceaps,
        "empresas_contratadas": empresas,
    }

    outputs = [
        _write_dataset(
            dataframe,
            root=silver_root,
            dataset=dataset,
        )
        for dataset, dataframe in datasets.items()
    ]

    approved = all(
        int(item["record_count"]) > 0 and int(item["duplicate_record_key_groups"]) == 0
        for item in outputs
    )

    manifest = {
        "source": "senado_federal",
        "layer": "silver",
        "years": [2025, 2026],
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "bronze_approved": True,
        "dataset_count": len(outputs),
        "record_count": sum(int(item["record_count"]) for item in outputs),
        "approved": approved,
        "datasets": outputs,
        "methodology_notes": [
            ("Arquivos CSV foram decodificados explicitamente como UTF-8 com BOM."),
            ("Estruturas JSON aninhadas foram achatadas sem eliminar campos."),
            ("Os votos foram extraidos do array interno de cada votacao."),
            ("Valores da CEAPS foram convertidos do formato decimal brasileiro."),
        ],
    }

    manifest_path = silver_root / "silver.manifest.json"

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

    print("=" * 100)
    print("SILVER DO SENADO FEDERAL")
    print("=" * 100)

    for item in outputs:
        print(
            f"{item['dataset']:<25} "
            f"registros={item['record_count']:<10} "
            f"colunas={item['column_count']:<5} "
            f"duplicadas="
            f"{item['duplicate_record_key_groups']}"
        )

    print()
    print(f"DATASET_COUNT={len(outputs)}")
    print(f"TOTAL_REGISTROS={manifest['record_count']}")
    print(f"SILVER_APPROVED={approved}")
    print(f"MANIFESTO={manifest_path}")

    return manifest_path
