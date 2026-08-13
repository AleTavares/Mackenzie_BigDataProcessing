#!/usr/bin/env python3
"""
Script de Geração de Datasets Sintéticos - Curso Big Data Processing
=====================================================================
Universidade Mackenzie - MBA em Engenharia de Dados

Gera todos os datasets sintéticos utilizados nas aulas do curso.
Usa seed fixa para reprodutibilidade e faker com locale pt_BR para
dados realistas brasileiros.

Uso:
    python gerar_datasets.py --all              # Gerar todos os datasets
    python gerar_datasets.py --aula 1           # Gerar apenas dados da Aula 1
    python gerar_datasets.py --aula 1 2 3       # Gerar dados das Aulas 1, 2 e 3
    python gerar_datasets.py --list             # Listar datasets disponíveis

Autor: DataFlow Analytics (Curso Big Data Processing)
"""

import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from faker import Faker

# ==============================================================================
# CONFIGURAÇÃO GLOBAL
# ==============================================================================

SEED = 42
SCRIPT_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = SCRIPT_DIR

# Inicializar Faker com locale brasileiro
fake = Faker("pt_BR")
Faker.seed(SEED)
np.random.seed(SEED)

# ==============================================================================
# CONSTANTES DO DOMÍNIO
# ==============================================================================

PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto"]
ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]
PARTNER_SOURCES = ["parceiro_a", "parceiro_b", "parceiro_c"]

BRAZILIAN_STATES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

# Pesos para estados (simula distribuição real de e-commerce)
# Normalizados para somar 1.0
_raw_state_weights = [
    0.005, 0.01, 0.003, 0.015, 0.07, 0.04, 0.03, 0.035, 0.03, 0.015,
    0.015, 0.012, 0.12, 0.02, 0.015, 0.08, 0.045, 0.01, 0.15, 0.012,
    0.08, 0.008, 0.003, 0.05, 0.25, 0.008, 0.005,
]
_total = sum(_raw_state_weights)
STATE_WEIGHTS = [w / _total for w in _raw_state_weights]

PRODUCT_CATEGORIES = [
    "Eletrônicos", "Moda", "Casa e Decoração", "Esportes",
    "Livros", "Saúde e Beleza", "Alimentos", "Brinquedos",
    "Automotivo", "Informática",
]

PRODUCT_SUBCATEGORIES = {
    "Eletrônicos": ["Smartphones", "TVs", "Áudio", "Câmeras", "Acessórios"],
    "Moda": ["Roupas Masculinas", "Roupas Femininas", "Calçados", "Acessórios", "Infantil"],
    "Casa e Decoração": ["Móveis", "Iluminação", "Tapetes", "Quadros", "Utilidades"],
    "Esportes": ["Fitness", "Futebol", "Corrida", "Natação", "Ciclismo"],
    "Livros": ["Ficção", "Técnicos", "Infantil", "Autoajuda", "Acadêmicos"],
    "Saúde e Beleza": ["Perfumes", "Maquiagem", "Cuidados Pessoais", "Vitaminas", "Cabelos"],
    "Alimentos": ["Bebidas", "Orgânicos", "Importados", "Doces", "Saudáveis"],
    "Brinquedos": ["Educativos", "Jogos", "Bonecas", "Veículos", "Eletrônicos"],
    "Automotivo": ["Peças", "Acessórios", "Óleos", "Ferramentas", "Som"],
    "Informática": ["Notebooks", "Periféricos", "Componentes", "Redes", "Software"],
}


# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def progress(message: str) -> None:
    """Imprime mensagem de progresso com timestamp."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def ensure_dir(path: Path) -> Path:
    """Cria diretório se não existir."""
    path.mkdir(parents=True, exist_ok=True)
    return path


# ==============================================================================
# GERADORES DE DADOS PRINCIPAIS
# ==============================================================================

def gerar_vendas(n_registros: int = 100_000, seed: int = SEED) -> pd.DataFrame:
    """
    Gera dataset de vendas sintéticas seguindo o schema principal do curso.

    Parâmetros:
        n_registros: Número de registros a gerar
        seed: Semente para reprodutibilidade

    Retorna:
        DataFrame pandas com vendas sintéticas
    """
    np.random.seed(seed)
    Faker.seed(seed)
    local_fake = Faker("pt_BR")

    progress(f"  Gerando {n_registros:,} registros de vendas...")

    # Gerar datas dentro do ano 2023
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_range_days = (end_date - start_date).days

    # Gerar dados em lote para performance
    # UUID reproduzível usando random com seed
    _rng = random.Random(seed)
    order_ids = [str(uuid.UUID(int=_rng.getrandbits(128))) for _ in range(n_registros)]
    customer_ids = [f"CUST_{np.random.randint(1, 50001):05d}" for _ in range(n_registros)]
    product_ids = [f"PROD_{np.random.randint(1, 5001):04d}" for _ in range(n_registros)]
    quantities = np.random.randint(1, 21, size=n_registros)
    unit_prices = np.round(np.random.uniform(10.0, 5000.0, size=n_registros), 2)
    total_amounts = np.round(quantities * unit_prices, 2)

    # Datas com distribuição mais realista (mais vendas no fim do ano)
    day_offsets = np.random.triangular(0, date_range_days * 0.7, date_range_days, size=n_registros)
    order_dates = [start_date + timedelta(days=int(d)) for d in day_offsets]

    # Métodos de pagamento com pesos realistas
    payment_weights = [0.35, 0.20, 0.30, 0.15]  # credit, debit, pix, boleto
    payment_methods = np.random.choice(PAYMENT_METHODS, size=n_registros, p=payment_weights)

    # Cidades e estados brasileiros
    states = np.random.choice(BRAZILIAN_STATES, size=n_registros, p=STATE_WEIGHTS)
    cities = [local_fake.city() for _ in range(n_registros)]

    # Status com pesos realistas
    status_weights = [0.10, 0.15, 0.65, 0.10]  # pending, shipped, delivered, cancelled
    statuses = np.random.choice(ORDER_STATUSES, size=n_registros, p=status_weights)

    # Partner source
    partner_weights = [0.40, 0.35, 0.25]
    partner_sources = np.random.choice(PARTNER_SOURCES, size=n_registros, p=partner_weights)

    df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "unit_price": unit_prices,
        "total_amount": total_amounts,
        "order_date": order_dates,
        "payment_method": payment_methods,
        "shipping_city": cities,
        "shipping_state": states,
        "status": statuses,
        "partner_source": partner_sources,
    })

    progress(f"  ✓ {len(df):,} vendas geradas")
    return df


def gerar_produtos(n_produtos: int = 5000, seed: int = SEED) -> pd.DataFrame:
    """
    Gera catálogo de produtos sintéticos.

    Parâmetros:
        n_produtos: Número de produtos a gerar
        seed: Semente para reprodutibilidade

    Retorna:
        DataFrame pandas com catálogo de produtos
    """
    np.random.seed(seed)
    Faker.seed(seed)
    local_fake = Faker("pt_BR")

    progress(f"  Gerando {n_produtos:,} produtos...")

    product_ids = [f"PROD_{i:04d}" for i in range(1, n_produtos + 1)]
    categories = np.random.choice(PRODUCT_CATEGORIES, size=n_produtos)
    subcategories = [
        np.random.choice(PRODUCT_SUBCATEGORIES[cat]) for cat in categories
    ]
    names = [local_fake.bs().title() for _ in range(n_produtos)]
    prices = np.round(np.random.lognormal(mean=4.5, sigma=1.2, size=n_produtos), 2)
    prices = np.clip(prices, 10.0, 5000.0)
    weights = np.round(np.random.uniform(0.1, 30.0, size=n_produtos), 2)
    active = np.random.choice([True, False], size=n_produtos, p=[0.85, 0.15])

    df = pd.DataFrame({
        "product_id": product_ids,
        "product_name": names,
        "category": categories,
        "subcategory": subcategories,
        "unit_price": prices,
        "weight_kg": weights,
        "is_active": active,
    })

    progress(f"  ✓ {len(df):,} produtos gerados")
    return df


def gerar_clientes(n_clientes: int = 500_000, seed: int = SEED) -> pd.DataFrame:
    """
    Gera dataset de clientes sintéticos brasileiros.

    Parâmetros:
        n_clientes: Número de clientes a gerar
        seed: Semente para reprodutibilidade

    Retorna:
        DataFrame pandas com dados de clientes
    """
    np.random.seed(seed)
    Faker.seed(seed)
    local_fake = Faker("pt_BR")

    progress(f"  Gerando {n_clientes:,} clientes...")

    customer_ids = [f"CUST_{i:05d}" for i in range(1, n_clientes + 1)]
    names = [local_fake.name() for _ in range(n_clientes)]
    emails = [local_fake.email() for _ in range(n_clientes)]
    phones = [local_fake.phone_number() for _ in range(n_clientes)]
    states = np.random.choice(BRAZILIAN_STATES, size=n_clientes, p=STATE_WEIGHTS)
    cities = [local_fake.city() for _ in range(n_clientes)]

    # Data de cadastro entre 2020 e 2023
    start_reg = datetime(2020, 1, 1)
    end_reg = datetime(2023, 12, 31)
    reg_range = (end_reg - start_reg).days
    reg_offsets = np.random.randint(0, reg_range, size=n_clientes)
    registration_dates = [start_reg + timedelta(days=int(d)) for d in reg_offsets]

    # Segmento de cliente
    segments = np.random.choice(
        ["Bronze", "Prata", "Ouro", "Platina"],
        size=n_clientes,
        p=[0.50, 0.30, 0.15, 0.05],
    )

    df = pd.DataFrame({
        "customer_id": customer_ids,
        "customer_name": names,
        "email": emails,
        "phone": phones,
        "city": cities,
        "state": states,
        "registration_date": registration_dates,
        "segment": segments,
    })

    progress(f"  ✓ {len(df):,} clientes gerados")
    return df


def gerar_categorias() -> dict:
    """
    Gera hierarquia de categorias de produtos (dataset fixo).

    Retorna:
        Dicionário com hierarquia de categorias
    """
    progress("  Gerando hierarquia de categorias...")

    categorias = []
    cat_id = 1
    for categoria in PRODUCT_CATEGORIES:
        subcats = []
        for sub in PRODUCT_SUBCATEGORIES[categoria]:
            subcats.append({
                "subcategory_id": f"SUBCAT_{cat_id:03d}",
                "subcategory_name": sub,
            })
            cat_id += 1
        categorias.append({
            "category_id": f"CAT_{PRODUCT_CATEGORIES.index(categoria) + 1:02d}",
            "category_name": categoria,
            "subcategories": subcats,
        })

    progress(f"  ✓ {len(categorias)} categorias com {cat_id - 1} subcategorias geradas")
    return {"categorias": categorias, "generated_at": datetime.now().isoformat()}


def gerar_dados_sujos(
    df: pd.DataFrame,
    pct_nulls: float = 0.05,
    pct_duplicatas: float = 0.03,
    seed: int = SEED,
) -> pd.DataFrame:
    """
    Introduz problemas de qualidade em um DataFrame para uso na Aula 6.

    Parâmetros:
        df: DataFrame original limpo
        pct_nulls: Percentual de valores nulos a introduzir (0.0 a 1.0)
        pct_duplicatas: Percentual de linhas duplicadas a adicionar
        seed: Semente para reprodutibilidade

    Retorna:
        DataFrame com problemas de qualidade intencionais
    """
    np.random.seed(seed)
    progress(f"  Introduzindo problemas de qualidade (nulls={pct_nulls:.0%}, duplicatas={pct_duplicatas:.0%})...")

    df_sujo = df.copy()
    n_rows = len(df_sujo)

    # 1. Introduzir valores nulos em colunas selecionadas
    nullable_cols = ["shipping_city", "shipping_state", "payment_method", "customer_id"]
    for col in nullable_cols:
        if col in df_sujo.columns:
            null_mask = np.random.random(n_rows) < pct_nulls
            df_sujo.loc[null_mask, col] = None

    # 2. Introduzir valores negativos em total_amount (dados inválidos)
    if "total_amount" in df_sujo.columns:
        invalid_mask = np.random.random(n_rows) < 0.01
        df_sujo.loc[invalid_mask, "total_amount"] = -np.abs(
            df_sujo.loc[invalid_mask, "total_amount"]
        )

    # 3. Introduzir duplicatas
    n_duplicatas = int(n_rows * pct_duplicatas)
    dup_indices = np.random.choice(n_rows, size=n_duplicatas, replace=True)
    df_duplicatas = df_sujo.iloc[dup_indices].copy()
    df_sujo = pd.concat([df_sujo, df_duplicatas], ignore_index=True)

    # 4. Introduzir inconsistências (quantity * unit_price != total_amount)
    if "quantity" in df_sujo.columns and "unit_price" in df_sujo.columns:
        inconsist_mask = np.random.random(len(df_sujo)) < 0.02
        df_sujo.loc[inconsist_mask, "total_amount"] = np.round(
            np.random.uniform(1, 10000, size=inconsist_mask.sum()), 2
        )

    # 5. Introduzir valores de status inválidos
    if "status" in df_sujo.columns:
        invalid_status_mask = np.random.random(len(df_sujo)) < 0.005
        df_sujo.loc[invalid_status_mask, "status"] = np.random.choice(
            ["INVALIDO", "erro", "NULL", ""], size=invalid_status_mask.sum()
        )

    # 6. Introduzir datas futuras inválidas
    if "order_date" in df_sujo.columns:
        future_mask = np.random.random(len(df_sujo)) < 0.008
        future_dates = [datetime(2025, 6, 15) + timedelta(days=int(d)) for d in range(future_mask.sum())]
        df_sujo.loc[future_mask, "order_date"] = future_dates[:future_mask.sum()]

    progress(f"  ✓ Dataset sujo gerado: {len(df_sujo):,} registros (original: {n_rows:,})")
    return df_sujo


def particionar_por_data(
    df: pd.DataFrame,
    output_dir: Path,
    date_col: str = "order_date",
    formato: str = "csv",
) -> None:
    """
    Particiona um DataFrame por data e salva em diretórios separados.

    Parâmetros:
        df: DataFrame a particionar
        output_dir: Diretório base de saída
        date_col: Nome da coluna de data
        formato: Formato de saída ('csv' ou 'parquet')
    """
    progress(f"  Particionando por {date_col} em {output_dir}...")

    df_copy = df.copy()
    df_copy["_partition_date"] = pd.to_datetime(df_copy[date_col]).dt.strftime("%Y-%m-%d")

    dates = df_copy["_partition_date"].unique()
    for date_str in sorted(dates):
        partition_dir = ensure_dir(output_dir / f"date={date_str}")
        df_partition = df_copy[df_copy["_partition_date"] == date_str].drop(
            columns=["_partition_date"]
        )

        if formato == "csv":
            df_partition.to_csv(partition_dir / "data.csv", index=False)
        elif formato == "parquet":
            df_partition.to_parquet(partition_dir / "data.parquet", index=False)

    progress(f"  ✓ {len(dates)} partições criadas em {output_dir}")


# ==============================================================================
# GERADORES POR AULA
# ==============================================================================

def gerar_aula_01() -> None:
    """Gera datasets para Aula 1: Fundamentos de Big Data e Apache Spark."""
    progress("=" * 60)
    progress("AULA 01: Fundamentos de Big Data e Apache Spark")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_01")

    # vendas_2023.csv - ~100K registros
    df_vendas = gerar_vendas(n_registros=100_000, seed=SEED)
    df_vendas.to_csv(output_dir / "vendas_2023.csv", index=False)
    progress(f"  → Salvo: {output_dir / 'vendas_2023.csv'}")

    # produtos.csv - ~5K produtos
    df_produtos = gerar_produtos(n_produtos=5_000, seed=SEED)
    df_produtos.to_csv(output_dir / "produtos.csv", index=False)
    progress(f"  → Salvo: {output_dir / 'produtos.csv'}")

    progress("AULA 01 concluída ✓\n")


def gerar_aula_02() -> None:
    """Gera datasets para Aula 2: Transformações Avançadas com Spark."""
    progress("=" * 60)
    progress("AULA 02: Transformações Avançadas com Spark")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_02")

    # vendas_2023_completo.parquet - ~1M registros
    df_vendas = gerar_vendas(n_registros=1_000_000, seed=SEED + 1)
    df_vendas.to_parquet(output_dir / "vendas_2023_completo.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'vendas_2023_completo.parquet'}")

    # clientes.parquet - ~500K clientes
    df_clientes = gerar_clientes(n_clientes=500_000, seed=SEED + 2)
    df_clientes.to_parquet(output_dir / "clientes.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'clientes.parquet'}")

    # categorias.json - hierarquia de categorias
    categorias = gerar_categorias()
    with open(output_dir / "categorias.json", "w", encoding="utf-8") as f:
        json.dump(categorias, f, ensure_ascii=False, indent=2)
    progress(f"  → Salvo: {output_dir / 'categorias.json'}")

    progress("AULA 02 concluída ✓\n")


def gerar_aula_03() -> None:
    """Gera datasets para Aula 3: Ingestão e Persistência de Dados."""
    progress("=" * 60)
    progress("AULA 03: Ingestão e Persistência de Dados")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_03")

    # Parceiro A: CSV legado com encoding ISO-8859-1 e separador ;
    parceiro_a_dir = ensure_dir(output_dir / "parceiro_a")
    df_a = gerar_vendas(n_registros=50_000, seed=SEED + 10)
    # Renomear colunas para simular sistema legado
    df_a = df_a.rename(columns={
        "order_id": "cod_pedido",
        "customer_id": "cod_cliente",
        "product_id": "cod_produto",
        "quantity": "qtd",
        "unit_price": "preco_unit",
        "total_amount": "valor_total",
        "order_date": "data_pedido",
        "payment_method": "forma_pagamento",
        "shipping_city": "cidade_entrega",
        "shipping_state": "uf_entrega",
        "status": "situacao",
        "partner_source": "origem",
    })
    # Salvar em 3 arquivos CSV separados (simulando exports mensais)
    df_a["mes"] = pd.to_datetime(df_a["data_pedido"]).dt.month
    for mes in [1, 6, 12]:
        subset = df_a[df_a["mes"] == mes].drop(columns=["mes"])
        filename = f"vendas_legacy_{mes:02d}_2023.csv"
        subset.to_csv(
            parceiro_a_dir / filename,
            index=False,
            sep=";",
            encoding="iso-8859-1",
        )
        progress(f"  → Salvo: {parceiro_a_dir / filename} (ISO-8859-1, sep=';')")

    # Parceiro B: JSON multi-arquivo (API dumps)
    parceiro_b_dir = ensure_dir(output_dir / "parceiro_b")
    df_b = gerar_vendas(n_registros=30_000, seed=SEED + 20)
    # Salvar em múltiplos arquivos JSON (simulando dumps de API)
    chunk_size = 10_000
    for i, start in enumerate(range(0, len(df_b), chunk_size)):
        chunk = df_b.iloc[start:start + chunk_size].copy()
        # Converter datetime para string para JSON
        chunk["order_date"] = chunk["order_date"].astype(str)
        records = chunk.to_dict(orient="records")
        payload = {
            "api_version": "2.1",
            "exported_at": "2023-12-15T10:30:00Z",
            "page": i + 1,
            "total_pages": (len(df_b) + chunk_size - 1) // chunk_size,
            "data": records,
        }
        filename = f"api_dump_page_{i + 1:03d}.json"
        with open(parceiro_b_dir / filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str)
        progress(f"  → Salvo: {parceiro_b_dir / filename}")

    # Parceiro C: Parquet otimizado
    parceiro_c_dir = ensure_dir(output_dir / "parceiro_c")
    df_c = gerar_vendas(n_registros=80_000, seed=SEED + 30)
    df_c.to_parquet(parceiro_c_dir / "vendas_parceiro_c.parquet", index=False)
    progress(f"  → Salvo: {parceiro_c_dir / 'vendas_parceiro_c.parquet'}")

    progress("AULA 03 concluída ✓\n")


def gerar_aula_04() -> None:
    """Gera datasets para Aula 4: Introdução ao Apache Airflow."""
    progress("=" * 60)
    progress("AULA 04: Introdução ao Apache Airflow")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_04" / "vendas_diarias")

    # Gerar 30 dias de vendas particionadas (simulando processamento diário)
    # Usa um subset menor para ser rápido no lab
    df_vendas = gerar_vendas(n_registros=30_000, seed=SEED + 40)

    # Filtrar para 30 dias específicos (novembro 2023)
    start = datetime(2023, 11, 1)
    end = datetime(2023, 11, 30)
    df_vendas["order_date"] = pd.to_datetime(df_vendas["order_date"])
    # Redistribuir datas para garantir dados em todos os 30 dias
    n = len(df_vendas)
    new_dates = [start + timedelta(days=int(d)) for d in np.random.randint(0, 30, size=n)]
    df_vendas["order_date"] = new_dates

    particionar_por_data(df_vendas, output_dir, date_col="order_date", formato="csv")

    progress("AULA 04 concluída ✓\n")


def gerar_aula_06() -> None:
    """Gera datasets para Aula 6: Qualidade de Dados e Monitoramento."""
    progress("=" * 60)
    progress("AULA 06: Qualidade de Dados e Monitoramento")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_06" / "dados_sujos")

    # Gerar vendas limpas primeiro
    df_vendas = gerar_vendas(n_registros=50_000, seed=SEED + 60)

    # Introduzir problemas de qualidade
    df_sujo = gerar_dados_sujos(df_vendas, pct_nulls=0.05, pct_duplicatas=0.03, seed=SEED + 61)

    # Salvar em CSV (mais fácil de visualizar os problemas)
    df_sujo.to_csv(output_dir / "vendas_problemas.csv", index=False)
    progress(f"  → Salvo: {output_dir / 'vendas_problemas.csv'}")

    # Salvar também em Parquet para exercício de comparação
    df_sujo.to_parquet(output_dir / "vendas_problemas.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'vendas_problemas.parquet'}")

    # Dataset limpo de referência (para o aluno comparar)
    df_vendas.to_parquet(output_dir / "vendas_referencia.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'vendas_referencia.parquet'}")

    progress("AULA 06 concluída ✓\n")


def gerar_aula_07() -> None:
    """Gera datasets para Aula 7: Pipeline End-to-End em Produção."""
    progress("=" * 60)
    progress("AULA 07: Pipeline End-to-End em Produção")
    progress("=" * 60)

    output_dir = ensure_dir(OUTPUT_DIR / "aula_07" / "producao")

    # Dataset completo para pipeline E2E
    # Vendas particionadas por data (simula incoming diário)
    df_vendas = gerar_vendas(n_registros=200_000, seed=SEED + 70)

    # Simular 7 dias de dados incoming
    incoming_dir = ensure_dir(output_dir / "incoming")
    df_vendas["order_date"] = pd.to_datetime(df_vendas["order_date"])

    # Redistribuir para 7 dias em dezembro 2023
    n = len(df_vendas)
    start = datetime(2023, 12, 1)
    new_dates = [start + timedelta(days=int(d)) for d in np.random.randint(0, 7, size=n)]
    df_vendas["order_date"] = new_dates

    # Particionar por data (simula arquivos chegando diariamente)
    for day_offset in range(7):
        day = start + timedelta(days=day_offset)
        day_str = day.strftime("%Y-%m-%d")
        day_dir = ensure_dir(incoming_dir / day_str)
        df_day = df_vendas[df_vendas["order_date"] == day]
        df_day.to_parquet(day_dir / "vendas.parquet", index=False)

    progress(f"  → Salvo: {incoming_dir} (7 dias de dados)")

    # Produtos e clientes para joins no pipeline
    df_produtos = gerar_produtos(n_produtos=5_000, seed=SEED + 71)
    df_produtos.to_parquet(output_dir / "produtos.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'produtos.parquet'}")

    df_clientes = gerar_clientes(n_clientes=50_000, seed=SEED + 72)
    df_clientes.to_parquet(output_dir / "clientes.parquet", index=False)
    progress(f"  → Salvo: {output_dir / 'clientes.parquet'}")

    progress("AULA 07 concluída ✓\n")


# ==============================================================================
# INTERFACE CLI
# ==============================================================================

AULA_GENERATORS = {
    1: gerar_aula_01,
    2: gerar_aula_02,
    3: gerar_aula_03,
    4: gerar_aula_04,
    6: gerar_aula_06,
    7: gerar_aula_07,
}

AULA_DESCRIPTIONS = {
    1: "Fundamentos de Big Data e Apache Spark (CSV 100K + Produtos 5K)",
    2: "Transformações Avançadas (Parquet 1M + Clientes 500K + Categorias)",
    3: "Ingestão Multi-formato (CSV legado + JSON API + Parquet)",
    4: "Introdução ao Airflow (Vendas particionadas por data - 30 dias)",
    6: "Qualidade de Dados (Dataset com problemas intencionais)",
    7: "Pipeline End-to-End (Dataset completo para produção)",
}


def listar_datasets() -> None:
    """Lista todos os datasets disponíveis para geração."""
    print("\n" + "=" * 60)
    print("DATASETS DISPONÍVEIS PARA GERAÇÃO")
    print("=" * 60)
    print(f"\nDiretório de saída: {OUTPUT_DIR}\n")
    for aula, desc in sorted(AULA_DESCRIPTIONS.items()):
        print(f"  Aula {aula:02d}: {desc}")
    print("\nUso:")
    print("  python gerar_datasets.py --all          # Todos os datasets")
    print("  python gerar_datasets.py --aula 1       # Apenas Aula 1")
    print("  python gerar_datasets.py --aula 1 2 3   # Aulas 1, 2 e 3")
    print()


def main() -> None:
    """Ponto de entrada principal com interface CLI."""
    parser = argparse.ArgumentParser(
        description="Gerador de Datasets Sintéticos - Curso Big Data Processing (Mackenzie)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python gerar_datasets.py --all              Gera todos os datasets
  python gerar_datasets.py --aula 1           Gera apenas datasets da Aula 1
  python gerar_datasets.py --aula 1 2 3       Gera datasets das Aulas 1, 2 e 3
  python gerar_datasets.py --list             Lista datasets disponíveis
        """,
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Gerar todos os datasets de todas as aulas",
    )
    parser.add_argument(
        "--aula",
        type=int,
        nargs="+",
        choices=sorted(AULA_GENERATORS.keys()),
        help="Número(s) da(s) aula(s) para gerar datasets",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Listar datasets disponíveis",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=SEED,
        help=f"Semente para reprodutibilidade (padrão: {SEED})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Diretório de saída (padrão: mesmo diretório do script)",
    )

    args = parser.parse_args()

    # Atualizar diretório de saída se especificado
    global OUTPUT_DIR
    if args.output_dir:
        OUTPUT_DIR = Path(args.output_dir).resolve()

    # Atualizar seed global se diferente
    if args.seed != SEED:
        np.random.seed(args.seed)
        Faker.seed(args.seed)

    # Listar datasets
    if args.list:
        listar_datasets()
        return

    # Validar argumentos
    if not args.all and not args.aula:
        parser.print_help()
        print("\n⚠️  Especifique --all ou --aula N para gerar datasets.")
        sys.exit(1)

    # Executar geração
    start_time = datetime.now()
    print()
    progress("=" * 60)
    progress("GERADOR DE DATASETS - CURSO BIG DATA PROCESSING")
    progress(f"Seed: {args.seed} | Output: {OUTPUT_DIR}")
    progress("=" * 60)
    print()

    aulas_to_generate = sorted(AULA_GENERATORS.keys()) if args.all else sorted(args.aula)

    for aula_num in aulas_to_generate:
        if aula_num in AULA_GENERATORS:
            AULA_GENERATORS[aula_num]()
        else:
            progress(f"⚠️  Aula {aula_num} não possui gerador de datasets.")

    # Resumo final
    elapsed = datetime.now() - start_time
    print()
    progress("=" * 60)
    progress(f"GERAÇÃO CONCLUÍDA em {elapsed.total_seconds():.1f}s")
    progress(f"Aulas geradas: {aulas_to_generate}")
    progress(f"Output: {OUTPUT_DIR}")
    progress("=" * 60)


if __name__ == "__main__":
    main()
