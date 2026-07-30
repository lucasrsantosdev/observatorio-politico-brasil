from __future__ import annotations

import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path.cwd()

SEMANTIC_ROOT = PROJECT_ROOT / "painel_portal_transparencia.SemanticModel"

DEFINITION_ROOT = SEMANTIC_ROOT / "definition"
TABLES_ROOT = DEFINITION_ROOT / "tables"
MODEL_PATH = DEFINITION_ROOT / "model.tmdl"

POWER_BI_ROOT = PROJECT_ROOT / "output" / "power_bi" / "senado_federal"

BACKUP_ROOT = (
    PROJECT_ROOT
    / "output"
    / "backup_modelo_semantico"
    / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
)


DATASETS = (
    "senado_fato_materias",
    "senado_fato_votacoes",
    "senado_fato_votos",
    "senado_fato_gastos_senadores",
    "senado_dim_senadores_base",
    "senado_dim_empresas_contratadas_base",
    "senado_ranking_senadores_gastos",
    "senado_ranking_fornecedores_ceaps",
    "senado_ranking_tipos_despesa_ceaps",
    "senado_ranking_senadores_votos",
    "senado_ranking_partidos_votos",
    "senado_resumo_gastos_mensal",
    "senado_resumo_atividade_mensal",
    "senado_dim_senador",
    "senado_dim_materia",
    "senado_dim_partido",
    "senado_dim_uf",
    "senado_dim_tipo_materia",
    "senado_dim_tipo_voto",
    "senado_dim_resultado_votacao",
    "senado_dim_tipo_despesa",
    "senado_dim_fornecedor",
    "senado_dim_tempo",
)


def tmdl_name(value: str) -> str:
    if re.search(r"[.=: '\s]", value):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    return value


def m_text(value: str) -> str:
    return value.replace('"', '""')


def map_dtype(dtype: pl.DataType) -> str:
    dtype_name = str(dtype)

    if dtype_name.startswith(
        (
            "Int",
            "UInt",
        )
    ):
        return "int64"

    if dtype_name.startswith(
        (
            "Float",
            "Decimal",
        )
    ):
        return "double"

    if dtype_name == "Boolean":
        return "boolean"

    if dtype_name == "Date":
        return "dateTime"

    if dtype_name.startswith("Datetime"):
        return "dateTime"

    if dtype_name.startswith("Duration"):
        return "int64"

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
            "reembolsado",
            "total",
            "ticket",
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


def is_key_candidate(
    table: str,
    column: str,
) -> bool:
    candidates = {
        "senado_dim_senador": {
            "id_senador",
        },
        "senado_dim_materia": {
            "id_materia",
        },
        "senado_dim_partido": {
            "id_partido",
        },
        "senado_dim_uf": {
            "id_uf",
        },
        "senado_dim_tipo_materia": {
            "id_tipo_materia",
        },
        "senado_dim_tipo_voto": {
            "id_tipo_voto",
        },
        "senado_dim_resultado_votacao": {
            "id_resultado_votacao",
        },
        "senado_dim_tipo_despesa": {
            "id_tipo_despesa",
        },
        "senado_dim_fornecedor": {
            "id_fornecedor",
        },
        "senado_dim_tempo": {
            "id_tempo",
        },
    }

    return column in candidates.get(
        table,
        set(),
    )


def build_table_tmdl(
    table: str,
    parquet_path: Path,
) -> str:
    schema = pl.scan_parquet(parquet_path).collect_schema()

    lines = [
        f"table {tmdl_name(table)}",
        "",
    ]

    for column, dtype in schema.items():
        data_type = map_dtype(dtype)

        lines.extend(
            [
                (f"\tcolumn {tmdl_name(column)}"),
                f"\t\tdataType: {data_type}",
            ]
        )

        if is_key_candidate(
            table,
            column,
        ):
            lines.append("\t\tisKey")

        lines.extend(
            [
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
            (f"\tpartition {tmdl_name(table)} = m"),
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
            ("\tannotation PBI_ResultType = Table"),
            "",
        ]
    )

    return "\n".join(lines)


def build_measures_tmdl() -> str:
    return """table _Medidas

\tmeasure 'Total Gastos Senado' =
\t\t\tSUM (
\t\t\t    'senado_fato_gastos_senadores'[valor_gasto]
\t\t\t)
\t\tformatString: R$ #,0.00;-R$ #,0.00;R$ #,0.00
\t\tdisplayFolder: Senado\\Gastos

\tmeasure 'Quantidade Despesas Senado' =
\t\t\tCOUNTROWS (
\t\t\t    'senado_fato_gastos_senadores'
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Gastos

\tmeasure 'Quantidade Senadores com Gastos' =
\t\t\tDISTINCTCOUNT (
\t\t\t    'senado_fato_gastos_senadores'[cod_senador]
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Gastos

\tmeasure 'Quantidade Fornecedores Senado' =
\t\t\tDISTINCTCOUNT (
\t\t\t    'senado_fato_gastos_senadores'[cpf_cnpj_fornecedor_normalizado]
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Gastos

\tmeasure 'Ticket Médio Despesa Senado' =
\t\t\tDIVIDE (
\t\t\t    [Total Gastos Senado],
\t\t\t    [Quantidade Despesas Senado],
\t\t\t    0
\t\t\t)
\t\tformatString: R$ #,0.00;-R$ #,0.00;R$ #,0.00
\t\tdisplayFolder: Senado\\Gastos

\tmeasure 'Total Matérias Senado' =
\t\t\tDISTINCTCOUNT (
\t\t\t    'senado_fato_materias'[id_materia]
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Total Votações Senado' =
\t\t\tDISTINCTCOUNT (
\t\t\t    'senado_fato_votacoes'[id_votacao]
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Total Votos Senado' =
\t\t\tCOUNTROWS (
\t\t\t    'senado_fato_votos'
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Votos Sim Senado' =
\t\t\tCALCULATE (
\t\t\t    [Total Votos Senado],
\t\t\t    'senado_fato_votos'[categoria_voto] = "SIM"
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Votos Não Senado' =
\t\t\tCALCULATE (
\t\t\t    [Total Votos Senado],
\t\t\t    'senado_fato_votos'[categoria_voto] = "NAO"
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Abstenções Senado' =
\t\t\tCALCULATE (
\t\t\t    [Total Votos Senado],
\t\t\t    'senado_fato_votos'[categoria_voto] = "ABSTENCAO"
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Percentual Votos Sim Senado' =
\t\t\tDIVIDE (
\t\t\t    [Votos Sim Senado],
\t\t\t    [Total Votos Senado],
\t\t\t    0
\t\t\t)
\t\tformatString: 0.00%
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Votações Aprovadas Senado' =
\t\t\tCALCULATE (
\t\t\t    [Total Votações Senado],
\t\t\t    'senado_fato_votacoes'[resultado_votacao_normalizado] = "APROVADA"
\t\t\t)
\t\tformatString: #,0
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tmeasure 'Percentual Votações Aprovadas Senado' =
\t\t\tDIVIDE (
\t\t\t    [Votações Aprovadas Senado],
\t\t\t    [Total Votações Senado],
\t\t\t    0
\t\t\t)
\t\tformatString: 0.00%
\t\tdisplayFolder: Senado\\Atividade Legislativa

\tcolumn Indicador
\t\tdataType: string
\t\tisHidden
\t\tsourceColumn: Indicador
\t\tsummarizeBy: none

\tpartition _Medidas = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = #table(
\t\t\t\t    type table [Indicador = text],
\t\t\t\t    {{"Medidas"}}
\t\t\t\t)
\t\t\tin
\t\t\t\tSource

\tannotation PBI_ResultType = Table
"""


def update_model_references(
    table_names: list[str],
) -> None:
    content = MODEL_PATH.read_text(encoding="utf-8")

    content = re.sub(
        r"(?m)^ref table .*\r?\n?",
        "",
        content,
    ).rstrip()

    references = "\n".join(f"ref table {tmdl_name(table)}" for table in table_names)

    updated = content + "\n\n" + references + "\n"

    MODEL_PATH.write_text(
        updated,
        encoding="utf-8",
    )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"model.tmdl nao encontrado: {MODEL_PATH}")

    missing = [
        dataset
        for dataset in DATASETS
        if not (POWER_BI_ROOT / f"{dataset}.parquet").exists()
    ]

    if missing:
        raise RuntimeError("Parquets ausentes: " + ", ".join(missing))

    BACKUP_ROOT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    shutil.copytree(
        SEMANTIC_ROOT,
        BACKUP_ROOT,
    )

    TABLES_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    generated: list[dict[str, Any]] = []

    for table in DATASETS:
        parquet_path = POWER_BI_ROOT / f"{table}.parquet"

        table_path = TABLES_ROOT / f"{table}.tmdl"

        table_path.write_text(
            build_table_tmdl(
                table,
                parquet_path,
            ),
            encoding="utf-8",
        )

        schema = pl.scan_parquet(parquet_path).collect_schema()

        generated.append(
            {
                "table": table,
                "columns": len(schema),
                "parquet": str(parquet_path),
                "tmdl": str(table_path),
            }
        )

    measures_path = TABLES_ROOT / "_Medidas.tmdl"

    if not measures_path.exists():
        measures_path.write_text(
            build_measures_tmdl(),
            encoding="utf-8",
        )

        print("TABELA_MEDIDAS_CRIADA")
    else:
        print("TABELA_MEDIDAS_PRESERVADA")

    all_tables = [
        *DATASETS,
        "_Medidas",
    ]

    update_model_references(all_tables)

    manifest = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "backup": str(BACKUP_ROOT),
        "table_count": len(DATASETS),
        "measure_table_created": True,
        "total_model_tables": len(all_tables),
        "tables": generated,
    }

    manifest_path = PROJECT_ROOT / "output" / "modelo_semantico_senado.manifest.json"

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
    print("MODELO SEMANTICO DO SENADO GERADO")
    print("=" * 100)

    for item in generated:
        print(f"{item['table']:<50} colunas={item['columns']}")

    print()
    print(f"TABELAS_DADOS={len(DATASETS)}")
    print("TABELA_MEDIDAS=1")
    print(f"TOTAL_TABELAS={len(all_tables)}")
    print(f"BACKUP={BACKUP_ROOT}")
    print(f"MANIFESTO={manifest_path}")


if __name__ == "__main__":
    main()
