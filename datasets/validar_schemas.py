#!/usr/bin/env python3
"""
Script de Validação de Schemas - Curso Big Data Processing
===========================================================
Universidade Mackenzie - MBA em Engenharia de Dados

Valida que todos os datasets gerados estão em conformidade com o schema
definido no design.md do curso.

Uso:
    python validar_schemas.py

Autor: DataFlow Analytics (Curso Big Data Processing)
"""

import json
import sys
from pathlib import Path

import pandas as pd

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()

# Schema principal de vendas (12 colunas)
VENDAS_COLUMNS = [
    "order_id", "customer_id", "product_id", "quantity",
    "unit_price", "total_amount", "order_date", "payment_method",
    "shipping_city", "shipping_state", "status", "partner_source",
]

# Parceiro A - colunas renomeadas (sistema legado)
PARCEIRO_A_COLUMNS = [
    "cod_pedido", "cod_cliente", "cod_produto", "qtd",
    "preco_unit", "valor_total", "data_pedido", "forma_pagamento",
    "cidade_entrega", "uf_entrega", "situacao", "origem",
]

# Schema de produtos
PRODUTOS_COLUMNS = [
    "product_id", "product_name", "category", "subcategory",
    "unit_price", "weight_kg", "is_active",
]

# Schema de clientes
CLIENTES_COLUMNS = [
    "customer_id", "customer_name", "email", "phone",
    "city", "state", "registration_date", "segment",
]

# Valores válidos
VALID_STATUSES = {"pending", "shipped", "delivered", "cancelled"}
VALID_PAYMENT_METHODS = {"credit_card", "debit_card", "pix", "boleto"}


# ==============================================================================
# FUNÇÕES DE VALIDAÇÃO
# ==============================================================================

class ValidationResult:
    """Resultado de uma validação individual."""

    def __init__(self, name: str):
        self.name = name
        self.passed = True
        self.messages: list[str] = []

    def fail(self, msg: str):
        self.passed = False
        self.messages.append(f"  ✗ {msg}")

    def warn(self, msg: str):
        self.messages.append(f"  ⚠ {msg}")

    def ok(self, msg: str):
        self.messages.append(f"  ✓ {msg}")

    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        header = f"[{status}] {self.name}"
        if self.messages:
            return header + "\n" + "\n".join(self.messages)
        return header


def validate_columns(df: pd.DataFrame, expected: list[str], result: ValidationResult) -> bool:
    """Verifica se o DataFrame contém exatamente as colunas esperadas."""
    actual = list(df.columns)
    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)

    if missing:
        result.fail(f"Colunas faltando: {sorted(missing)}")
        return False
    if extra:
        result.warn(f"Colunas extras (não esperadas): {sorted(extra)}")

    result.ok(f"Todas as {len(expected)} colunas esperadas presentes")
    return True


def validate_vendas_rules(df: pd.DataFrame, result: ValidationResult, allow_dirty: bool = False) -> None:
    """Valida regras de negócio do schema de vendas."""
    # Regra: quantity > 0
    if "quantity" in df.columns:
        invalid_qty = (df["quantity"] <= 0).sum()
        if invalid_qty > 0 and not allow_dirty:
            result.fail(f"quantity <= 0: {invalid_qty} registros")
        elif invalid_qty > 0 and allow_dirty:
            result.warn(f"quantity <= 0: {invalid_qty} registros (esperado em dados sujos)")
        else:
            result.ok("quantity > 0 para todos os registros")

    # Regra: unit_price >= 0
    if "unit_price" in df.columns:
        invalid_price = (df["unit_price"] < 0).sum()
        if invalid_price > 0 and not allow_dirty:
            result.fail(f"unit_price < 0: {invalid_price} registros")
        elif invalid_price > 0 and allow_dirty:
            result.warn(f"unit_price < 0: {invalid_price} registros (esperado em dados sujos)")
        else:
            result.ok("unit_price >= 0 para todos os registros")

    # Regra: status ∈ valores válidos
    if "status" in df.columns:
        invalid_status = df[~df["status"].isin(VALID_STATUSES) & df["status"].notna()]
        n_invalid = len(invalid_status)
        if n_invalid > 0 and not allow_dirty:
            result.fail(f"status inválido: {n_invalid} registros (ex: {invalid_status['status'].unique()[:5]})")
        elif n_invalid > 0 and allow_dirty:
            result.warn(f"status inválido: {n_invalid} registros (esperado em dados sujos)")
        else:
            result.ok("status válido para todos os registros")

    # Regra: payment_method ∈ valores válidos (ou null)
    if "payment_method" in df.columns:
        non_null_payments = df["payment_method"].dropna()
        invalid_pm = non_null_payments[~non_null_payments.isin(VALID_PAYMENT_METHODS)]
        n_invalid = len(invalid_pm)
        if n_invalid > 0 and not allow_dirty:
            result.fail(f"payment_method inválido: {n_invalid} registros")
        elif n_invalid > 0 and allow_dirty:
            result.warn(f"payment_method inválido: {n_invalid} registros (esperado em dados sujos)")
        else:
            result.ok("payment_method válido para todos os registros")


# ==============================================================================
# VALIDAÇÕES POR AULA
# ==============================================================================

def validar_aula_01() -> list[ValidationResult]:
    """Valida datasets da Aula 01."""
    results = []

    # vendas_2023.csv
    r = ValidationResult("Aula 01 - vendas_2023.csv")
    filepath = SCRIPT_DIR / "aula_01" / "vendas_2023.csv"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_csv(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
        validate_vendas_rules(df, r)
    results.append(r)

    return results


def validar_aula_02() -> list[ValidationResult]:
    """Valida datasets da Aula 02."""
    results = []

    # vendas_2023_completo.parquet
    r = ValidationResult("Aula 02 - vendas_2023_completo.parquet")
    filepath = SCRIPT_DIR / "aula_02" / "vendas_2023_completo.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
        validate_vendas_rules(df, r)
    results.append(r)

    # clientes.parquet
    r = ValidationResult("Aula 02 - clientes.parquet")
    filepath = SCRIPT_DIR / "aula_02" / "clientes.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, CLIENTES_COLUMNS, r)
    results.append(r)

    # categorias.json
    r = ValidationResult("Aula 02 - categorias.json")
    filepath = SCRIPT_DIR / "aula_02" / "categorias.json"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "categorias" not in data:
            r.fail("Campo 'categorias' não encontrado no JSON")
        else:
            cats = data["categorias"]
            r.ok(f"JSON lido: {len(cats)} categorias")
            # Validar estrutura de cada categoria
            for cat in cats[:1]:  # Verificar pelo menos a primeira
                required_keys = {"category_id", "category_name", "subcategories"}
                if not required_keys.issubset(set(cat.keys())):
                    r.fail(f"Categoria com chaves faltando: {required_keys - set(cat.keys())}")
                else:
                    r.ok("Estrutura de categorias válida (category_id, category_name, subcategories)")
                    # Validar subcategorias
                    if cat["subcategories"]:
                        sub_keys = {"subcategory_id", "subcategory_name"}
                        if sub_keys.issubset(set(cat["subcategories"][0].keys())):
                            r.ok("Estrutura de subcategorias válida (subcategory_id, subcategory_name)")
                        else:
                            r.fail(f"Subcategoria com chaves faltando")
    results.append(r)

    return results


def validar_aula_03() -> list[ValidationResult]:
    """Valida datasets da Aula 03."""
    results = []

    # Parceiro A - CSV legado com colunas renomeadas
    parceiro_a_dir = SCRIPT_DIR / "aula_03" / "parceiro_a"
    r = ValidationResult("Aula 03 - Parceiro A (CSV legado)")
    if not parceiro_a_dir.exists():
        r.fail(f"Diretório não encontrado: {parceiro_a_dir}")
    else:
        csv_files = list(parceiro_a_dir.glob("*.csv"))
        if not csv_files:
            r.fail("Nenhum arquivo CSV encontrado")
        else:
            r.ok(f"{len(csv_files)} arquivo(s) CSV encontrado(s)")
            # Ler o primeiro arquivo para validar schema
            df = pd.read_csv(csv_files[0], sep=";", encoding="iso-8859-1")
            r.ok(f"Arquivo lido ({csv_files[0].name}): {len(df):,} registros")
            validate_columns(df, PARCEIRO_A_COLUMNS, r)
            # Validar regras com colunas renomeadas
            if "qtd" in df.columns:
                invalid_qty = (df["qtd"] <= 0).sum()
                if invalid_qty > 0:
                    r.fail(f"qtd <= 0: {invalid_qty} registros")
                else:
                    r.ok("qtd > 0 para todos os registros")
            if "preco_unit" in df.columns:
                invalid_price = (df["preco_unit"] < 0).sum()
                if invalid_price > 0:
                    r.fail(f"preco_unit < 0: {invalid_price} registros")
                else:
                    r.ok("preco_unit >= 0 para todos os registros")
    results.append(r)

    # Parceiro B - JSON multi-arquivo
    parceiro_b_dir = SCRIPT_DIR / "aula_03" / "parceiro_b"
    r = ValidationResult("Aula 03 - Parceiro B (JSON API)")
    if not parceiro_b_dir.exists():
        r.fail(f"Diretório não encontrado: {parceiro_b_dir}")
    else:
        json_files = list(parceiro_b_dir.glob("*.json"))
        if not json_files:
            r.fail("Nenhum arquivo JSON encontrado")
        else:
            r.ok(f"{len(json_files)} arquivo(s) JSON encontrado(s)")
            # Validar estrutura do primeiro arquivo
            with open(json_files[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            if "data" not in data:
                r.fail("Campo 'data' não encontrado no JSON")
            else:
                records = data["data"]
                r.ok(f"Campo 'data' presente com {len(records):,} registros")
                if records:
                    df = pd.DataFrame(records)
                    validate_columns(df, VENDAS_COLUMNS, r)
                    validate_vendas_rules(df, r)
                # Validar metadados
                meta_keys = {"api_version", "exported_at", "page", "total_pages"}
                present_meta = meta_keys.intersection(set(data.keys()))
                if present_meta == meta_keys:
                    r.ok("Metadados de API presentes (api_version, exported_at, page, total_pages)")
                else:
                    r.warn(f"Metadados faltando: {meta_keys - present_meta}")
    results.append(r)

    # Parceiro C - Parquet
    r = ValidationResult("Aula 03 - Parceiro C (Parquet)")
    filepath = SCRIPT_DIR / "aula_03" / "parceiro_c" / "vendas_parceiro_c.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
        validate_vendas_rules(df, r)
    results.append(r)

    return results


def validar_aula_04() -> list[ValidationResult]:
    """Valida datasets da Aula 04."""
    results = []

    r = ValidationResult("Aula 04 - vendas_diarias (particionado por data)")
    base_dir = SCRIPT_DIR / "aula_04" / "vendas_diarias"
    if not base_dir.exists():
        r.fail(f"Diretório não encontrado: {base_dir}")
    else:
        partition_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith("date=")]
        if not partition_dirs:
            r.fail("Nenhuma partição date= encontrada")
        else:
            r.ok(f"{len(partition_dirs)} partições encontradas")
            # Validar a primeira partição
            first_partition = sorted(partition_dirs)[0]
            csv_file = first_partition / "data.csv"
            if not csv_file.exists():
                r.fail(f"data.csv não encontrado em {first_partition.name}")
            else:
                df = pd.read_csv(csv_file)
                r.ok(f"Partição {first_partition.name}: {len(df):,} registros")
                validate_columns(df, VENDAS_COLUMNS, r)
                validate_vendas_rules(df, r)

            # Verificar que são 30 partições (novembro 2023)
            if len(partition_dirs) == 30:
                r.ok("Exatamente 30 partições (novembro 2023 completo)")
            else:
                r.warn(f"Esperado 30 partições, encontrado {len(partition_dirs)}")
    results.append(r)

    return results


def validar_aula_06() -> list[ValidationResult]:
    """Valida datasets da Aula 06."""
    results = []

    # vendas_problemas.csv (dados sujos - violações são esperadas)
    r = ValidationResult("Aula 06 - vendas_problemas.csv (dados sujos)")
    filepath = SCRIPT_DIR / "aula_06" / "dados_sujos" / "vendas_problemas.csv"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_csv(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
        validate_vendas_rules(df, r, allow_dirty=True)
    results.append(r)

    # vendas_problemas.parquet
    r = ValidationResult("Aula 06 - vendas_problemas.parquet (dados sujos)")
    filepath = SCRIPT_DIR / "aula_06" / "dados_sujos" / "vendas_problemas.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
    results.append(r)

    # vendas_referencia.parquet (limpo)
    r = ValidationResult("Aula 06 - vendas_referencia.parquet (referência limpa)")
    filepath = SCRIPT_DIR / "aula_06" / "dados_sujos" / "vendas_referencia.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, VENDAS_COLUMNS, r)
        validate_vendas_rules(df, r)
    results.append(r)

    return results


def validar_aula_07() -> list[ValidationResult]:
    """Valida datasets da Aula 07."""
    results = []

    # incoming (vendas particionadas por data)
    r = ValidationResult("Aula 07 - producao/incoming (7 dias)")
    incoming_dir = SCRIPT_DIR / "aula_07" / "producao" / "incoming"
    if not incoming_dir.exists():
        r.fail(f"Diretório não encontrado: {incoming_dir}")
    else:
        day_dirs = [d for d in incoming_dir.iterdir() if d.is_dir()]
        if not day_dirs:
            r.fail("Nenhum diretório de dia encontrado")
        else:
            r.ok(f"{len(day_dirs)} diretório(s) de dia encontrado(s)")
            # Validar primeiro dia
            first_day = sorted(day_dirs)[0]
            parquet_file = first_day / "vendas.parquet"
            if not parquet_file.exists():
                r.fail(f"vendas.parquet não encontrado em {first_day.name}")
            else:
                df = pd.read_parquet(parquet_file)
                r.ok(f"Dia {first_day.name}: {len(df):,} registros")
                validate_columns(df, VENDAS_COLUMNS, r)
                validate_vendas_rules(df, r)

            if len(day_dirs) == 7:
                r.ok("Exatamente 7 dias de dados incoming")
            else:
                r.warn(f"Esperado 7 dias, encontrado {len(day_dirs)}")
    results.append(r)

    # produtos.parquet
    r = ValidationResult("Aula 07 - producao/produtos.parquet")
    filepath = SCRIPT_DIR / "aula_07" / "producao" / "produtos.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, PRODUTOS_COLUMNS, r)
    results.append(r)

    # clientes.parquet
    r = ValidationResult("Aula 07 - producao/clientes.parquet")
    filepath = SCRIPT_DIR / "aula_07" / "producao" / "clientes.parquet"
    if not filepath.exists():
        r.fail(f"Arquivo não encontrado: {filepath}")
    else:
        df = pd.read_parquet(filepath)
        r.ok(f"Arquivo lido: {len(df):,} registros")
        validate_columns(df, CLIENTES_COLUMNS, r)
    results.append(r)

    return results


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def main() -> None:
    """Executa todas as validações e imprime relatório."""
    print("=" * 70)
    print("VALIDAÇÃO DE SCHEMAS - CURSO BIG DATA PROCESSING")
    print("=" * 70)
    print(f"\nDiretório base: {SCRIPT_DIR}\n")

    all_results: list[ValidationResult] = []

    validators = [
        ("AULA 01", validar_aula_01),
        ("AULA 02", validar_aula_02),
        ("AULA 03", validar_aula_03),
        ("AULA 04", validar_aula_04),
        ("AULA 06", validar_aula_06),
        ("AULA 07", validar_aula_07),
    ]

    for section_name, validator_fn in validators:
        print(f"\n{'─' * 50}")
        print(f"  {section_name}")
        print(f"{'─' * 50}")
        results = validator_fn()
        for r in results:
            print(f"\n{r}")
        all_results.extend(results)

    # Resumo final
    total = len(all_results)
    passed = sum(1 for r in all_results if r.passed)
    failed = total - passed

    print(f"\n\n{'=' * 70}")
    print("RESUMO DA VALIDAÇÃO")
    print(f"{'=' * 70}")
    print(f"\n  Total de validações: {total}")
    print(f"  Aprovadas: {passed} ✓")
    print(f"  Reprovadas: {failed} ✗")
    print(f"\n  Resultado: {'TODOS OS SCHEMAS VÁLIDOS ✓' if failed == 0 else 'FALHAS DETECTADAS ✗'}")
    print(f"{'=' * 70}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
