from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path.cwd()

POWER_BI_ROOT = PROJECT_ROOT / "output" / "power_bi" / "senado_federal"

DEFINITION_ROOT = (
    PROJECT_ROOT / "painel_portal_transparencia.SemanticModel" / "definition"
)

RELATIONSHIPS_PATH = DEFINITION_ROOT / "relationships.tmdl"

AUDIT_PATH = (
    PROJECT_ROOT
    / "output"
    / "modelo_semantico"
    / "senado_relationships_validation.json"
)


RELATIONSHIPS: tuple[dict[str, Any], ...] = (
    {
        "name": "senador_gastos",
        "from_table": "senado_fato_gastos_senadores",
        "from_column": "cod_senador",
        "to_table": "senado_dim_senador",
        "to_column": "id_senador",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "senador_votos",
        "from_table": "senado_fato_votos",
        "from_column": "codigo_parlamentar",
        "to_table": "senado_dim_senador",
        "to_column": "id_senador",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "materia_votacoes",
        "from_table": "senado_fato_votacoes",
        "from_column": "codigo_materia",
        "to_table": "senado_dim_materia",
        "to_column": "id_materia",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
    {
        "name": "materia_votos",
        "from_table": "senado_fato_votos",
        "from_column": "codigo_materia",
        "to_table": "senado_dim_materia",
        "to_column": "id_materia",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
    {
        "name": "tempo_gastos",
        "from_table": "senado_fato_gastos_senadores",
        "from_column": "data_referencia",
        "to_table": "senado_dim_tempo",
        "to_column": "data",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "tempo_votacoes",
        "from_table": "senado_fato_votacoes",
        "from_column": "data_sessao",
        "to_table": "senado_dim_tempo",
        "to_column": "data",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "tempo_votos",
        "from_table": "senado_fato_votos",
        "from_column": "data_sessao",
        "to_table": "senado_dim_tempo",
        "to_column": "data",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "tempo_materias",
        "from_table": "senado_fato_materias",
        "from_column": "data",
        "to_table": "senado_dim_tempo",
        "to_column": "data",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
    {
        "name": "tipo_despesa_gastos",
        "from_table": "senado_fato_gastos_senadores",
        "from_column": "tipo_despesa",
        "to_table": "senado_dim_tipo_despesa",
        "to_column": "tipo_despesa",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "tipo_materia_materias",
        "from_table": "senado_fato_materias",
        "from_column": "sigla",
        "to_table": "senado_dim_tipo_materia",
        "to_column": "sigla_tipo_materia",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "resultado_votacoes",
        "from_table": "senado_fato_votacoes",
        "from_column": "resultado_votacao_normalizado",
        "to_table": "senado_dim_resultado_votacao",
        "to_column": "resultado_normalizado",
        "from_cardinality": "many",
        "to_cardinality": "one",
    },
    {
        "name": "partido_votos",
        "from_table": "senado_fato_votos",
        "from_column": "sigla_partido_parlamentar",
        "to_table": "senado_dim_partido",
        "to_column": "sigla_partido",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
    {
        "name": "uf_votos",
        "from_table": "senado_fato_votos",
        "from_column": "sigla_uf_parlamentar",
        "to_table": "senado_dim_uf",
        "to_column": "sigla_uf",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
    {
        "name": "tipo_voto_votos",
        "from_table": "senado_fato_votos",
        "from_column": "sigla_voto_parlamentar",
        "to_table": "senado_dim_tipo_voto",
        "to_column": "descricao_voto_original",
        "from_cardinality": "many",
        "to_cardinality": "one",
        "allow_orphans": True,
    },
)


def parquet_path(table: str) -> Path:
    path = POWER_BI_ROOT / f"{table}.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Parquet nao encontrado: {path}")

    return path


def read_columns(
    table: str,
    columns: list[str],
) -> pl.DataFrame:
    return pl.scan_parquet(parquet_path(table)).select(columns).collect()


def relationship_id(name: str) -> str:
    namespace = uuid.UUID("91be2c29-69a9-4aaf-92c1-e45e0839f341")

    return str(uuid.uuid5(namespace, f"senado:{name}"))


def tmdl_reference(
    table: str,
    column: str,
) -> str:
    return f"{table}.{column}"


def validate_relationship(
    relationship: dict[str, Any],
) -> dict[str, Any]:
    from_table = str(relationship["from_table"])

    from_column = str(relationship["from_column"])

    to_table = str(relationship["to_table"])

    to_column = str(relationship["to_column"])

    from_data = read_columns(
        from_table,
        [from_column],
    )

    to_data = read_columns(
        to_table,
        [to_column],
    )

    duplicate_to_keys = (
        to_data.drop_nulls().group_by(to_column).len().filter(pl.col("len") > 1)
    )

    duplicate_from_keys = (
        from_data.drop_nulls().group_by(from_column).len().filter(pl.col("len") > 1)
    )

    orphan_rows = from_data.drop_nulls().join(
        to_data.drop_nulls().unique(),
        left_on=from_column,
        right_on=to_column,
        how="anti",
    )

    from_dtype = str(from_data.schema[from_column])

    to_dtype = str(to_data.schema[to_column])

    to_unique = duplicate_to_keys.height == 0

    from_unique = duplicate_from_keys.height == 0

    cardinality_valid = to_unique and (
        relationship["from_cardinality"] != "one" or from_unique
    )

    orphans_allowed = bool(
        relationship.get(
            "allow_orphans",
            False,
        )
    )

    approved = (
        cardinality_valid
        and from_dtype == to_dtype
        and (orphan_rows.height == 0 or orphans_allowed)
    )

    return {
        **relationship,
        "relationship_id": relationship_id(str(relationship["name"])),
        "from_rows": from_data.height,
        "to_rows": to_data.height,
        "from_dtype": from_dtype,
        "to_dtype": to_dtype,
        "from_unique": from_unique,
        "to_unique": to_unique,
        "duplicate_to_key_groups": (duplicate_to_keys.height),
        "orphan_rows": orphan_rows.height,
        "orphans_allowed": orphans_allowed,
        "approved": approved,
    }


def build_tmdl(
    validations: list[dict[str, Any]],
) -> str:
    blocks: list[str] = []

    for item in validations:
        blocks.extend(
            [
                (f"relationship {item['relationship_id']}"),
                (f"\tfromCardinality: {item['from_cardinality']}"),
                (f"\ttoCardinality: {item['to_cardinality']}"),
                (
                    "\tfromColumn: "
                    + tmdl_reference(
                        str(item["from_table"]),
                        str(item["from_column"]),
                    )
                ),
                (
                    "\ttoColumn: "
                    + tmdl_reference(
                        str(item["to_table"]),
                        str(item["to_column"]),
                    )
                ),
                "\tcrossFilteringBehavior: oneDirection",
                "",
            ]
        )

    return "\n".join(blocks)


def main() -> None:
    validations = [validate_relationship(item) for item in RELATIONSHIPS]

    rejected = [item for item in validations if not item["approved"]]

    AUDIT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "relationship_count": len(validations),
        "approved_count": sum(bool(item["approved"]) for item in validations),
        "rejected_count": len(rejected),
        "approved": not rejected,
        "relationships": validations,
    }

    AUDIT_PATH.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 120)
    print("VALIDACAO DOS RELACIONAMENTOS DO SENADO")
    print("=" * 120)

    for item in validations:
        status = "OK" if item["approved"] else "FALHOU"

        print(
            f"{status:<7} "
            f"{item['name']:<30} "
            f"{item['from_cardinality']}:"
            f"{item['to_cardinality']} "
            f"orfãos={item['orphan_rows']:<6} "
            f"duplicadas_dim="
            f"{item['duplicate_to_key_groups']}"
        )

    print()

    if rejected:
        print(f"RELACIONAMENTOS_REPROVADOS={len(rejected)}")

        for item in rejected:
            print(
                f"- {item['name']}: "
                f"from_dtype={item['from_dtype']} "
                f"to_dtype={item['to_dtype']} "
                f"to_unique={item['to_unique']} "
                f"orfaos={item['orphan_rows']}"
            )

        raise RuntimeError(f"Existem relacionamentos invalidos. Consulte {AUDIT_PATH}")

    RELATIONSHIPS_PATH.write_text(
        build_tmdl(validations),
        encoding="utf-8",
    )

    print(f"RELACIONAMENTOS_APROVADOS={len(validations)}")
    print(f"ARQUIVO={RELATIONSHIPS_PATH}")
    print(f"AUDITORIA={AUDIT_PATH}")


if __name__ == "__main__":
    main()
