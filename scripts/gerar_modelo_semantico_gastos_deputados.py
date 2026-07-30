from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path.cwd()

POWER_BI_ROOT = PROJECT_ROOT / "output" / "power_bi" / "gastos_deputados"

SEMANTIC_ROOT = PROJECT_ROOT / "painel_portal_transparencia.SemanticModel"

DEFINITION_ROOT = SEMANTIC_ROOT / "definition"
TABLES_ROOT = DEFINITION_ROOT / "tables"
MODEL_PATH = DEFINITION_ROOT / "model.tmdl"

MANIFEST_PATH = (
    PROJECT_ROOT / "output" / "modelo_semantico_gastos_deputados.manifest.json"
)


def tmdl_name(value: str) -> str:
    if re.search(r"[.=: '\s]", value):
        return "'" + value.replace("'", "''") + "'"

    return value


def m_text(value: str) -> str:
    return value.replace('"', '""')


def map_dtype(dtype: pl.DataType) -> str:
    name = str(dtype)

    if name.startswith(("Int", "UInt")):
        return "int64"

    if name.startswith(("Float", "Decimal")):
        return "double"

    if name == "Boolean":
        return "boolean"

    if name == "Date" or name.startswith("Datetime"):
        return "dateTime"

    if name == "Null":
        return "string"

    return "string"


def format_string(
    column: str,
    data_type: str,
) -> str | None:
    normalized = column.lower()

    if data_type == "dateTime":
        return "Short Date"

    if data_type == "double" and any(
        token in normalized
        for token in (
            "valor",
            "gasto",
            "despesa",
            "total",
            "ticket",
            "liquido",
            "glosa",
        )
    ):
        return "R$ #,0.00;-R$ #,0.00;R$ #,0.00"

    if data_type == "double" and any(
        token in normalized
        for token in (
            "percentual",
            "percent",
            "pct",
        )
    ):
        return "0.00%"

    return None


def build_table_tmdl(
    table: str,
    parquet_path: Path,
) -> str:
    schema = pl.read_parquet_schema(parquet_path)

    lines = [
        f"table {tmdl_name(table)}",
        "",
    ]

    for column, dtype in schema.items():
        data_type = map_dtype(dtype)

        lines.extend(
            [
                f"\tcolumn {tmdl_name(column)}",
                f"\t\tdataType: {data_type}",
                (f"\t\tsourceColumn: {tmdl_name(column)}"),
                "\t\tsummarizeBy: none",
            ]
        )

        column_format = format_string(
            column,
            data_type,
        )

        if column_format:
            lines.append(f"\t\tformatString: {column_format}")

        lines.append("")

    absolute_path = str(parquet_path.resolve())

    lines.extend(
        [
            f"\tpartition {tmdl_name(table)} = m",
            "\t\tmode: import",
            "\t\tsource =",
            "\t\t\tlet",
            (
                "\t\t\t\tSource = "
                "Parquet.Document("
                "File.Contents("
                f'"{m_text(absolute_path)}"'
                ")"
                ")"
            ),
            "\t\t\tin",
            "\t\t\t\tSource",
            "",
            "\tannotation PBI_ResultType = Table",
            "",
        ]
    )

    return "\n".join(lines)


def update_model_references(
    tables: list[str],
) -> None:
    content = MODEL_PATH.read_text(encoding="utf-8")

    existing = re.findall(
        r"(?m)^ref table (.+)$",
        content,
    )

    base = re.sub(
        r"(?m)^ref table .*\r?\n?",
        "",
        content,
    ).rstrip()

    references: list[str] = []

    for table in [
        *existing,
        *tables,
    ]:
        normalized = table.strip()

        if normalized not in references:
            references.append(normalized)

    reference_text = "\n".join(f"ref table {table}" for table in references)

    MODEL_PATH.write_text(
        base + "\n\n" + reference_text + "\n",
        encoding="utf-8",
    )


def main() -> None:
    if not POWER_BI_ROOT.exists():
        raise FileNotFoundError(f"Pasta nao encontrada: {POWER_BI_ROOT}")

    parquet_paths = sorted(POWER_BI_ROOT.glob("*.parquet"))

    if not parquet_paths:
        raise RuntimeError("Nenhum Parquet de gastos dos deputados.")

    TABLES_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    outputs: list[dict[str, Any]] = []

    for parquet_path in parquet_paths:
        table = parquet_path.stem

        schema = pl.read_parquet_schema(parquet_path)

        invalid_columns = [
            column for column, dtype in schema.items() if dtype == pl.Null
        ]

        if invalid_columns:
            raise RuntimeError(
                f"{table} possui colunas Null: " + ", ".join(invalid_columns)
            )

        tmdl_path = TABLES_ROOT / f"{table}.tmdl"

        tmdl_path.write_text(
            build_table_tmdl(
                table,
                parquet_path,
            ),
            encoding="utf-8",
        )

        row_count = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()

        outputs.append(
            {
                "table": table,
                "row_count": row_count,
                "column_count": len(schema),
                "parquet": str(parquet_path),
                "tmdl": str(tmdl_path),
            }
        )

    update_model_references([item["table"] for item in outputs])

    manifest = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "domain": "gastos_deputados",
        "table_count": len(outputs),
        "approved": True,
        "tables": outputs,
    }

    MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    MANIFEST_PATH.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 100)
    print("MODELO SEMANTICO — GASTOS DOS DEPUTADOS")
    print("=" * 100)

    for item in outputs:
        print(
            f"{item['table']:<65} "
            f"registros={item['row_count']:<10} "
            f"colunas={item['column_count']}"
        )

    print()
    print(f"TABELAS_CRIADAS={len(outputs)}")
    print("MODELO_GASTOS_DEPUTADOS_APROVADO=True")
    print(f"MANIFESTO={MANIFEST_PATH}")


if __name__ == "__main__":
    main()
