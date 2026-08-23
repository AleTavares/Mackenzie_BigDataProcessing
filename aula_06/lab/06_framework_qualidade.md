# Exercício 6 — Desafio: Framework Completo DataQualityFramework Reutilizável

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, você construiu checks individuais, quarentena, integração com Airflow... Funciona. Mas temos 50+ pipelines de clientes diferentes, e cada um re-implementa a mesma lógica de qualidade do zero. Quero um **framework reutilizável** — uma classe Python que qualquer engenheiro da equipe possa usar com 5 linhas de código. Instancia, configura os checks, executa, gera relatório. Se não passar no quality gate, levanta exceção. Simples assim."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Faz total sentido. Vou encapsular tudo numa classe `DataQualityFramework` com interface fluente. Cada método de check retorna um `CheckResult` padronizado. No final, `generate_report()` consolida tudo e `run_all_checks()` executa a bateria completa a partir de um dicionário de configuração. Vai ser o componente mais reutilizado do nosso stack."

## Objetivos

Ao final deste exercício, você terá construído:

- Uma classe `DataQualityFramework` completa e reutilizável
- Um dataclass `CheckResult` para padronizar resultados de validação
- Métodos para todos os tipos de check: completude, unicidade, integridade referencial e validade de domínio
- Sistema de quarentena integrado ao framework
- Execução em batch via configuração (`run_all_checks`)
- Geração de relatório consolidado com quality gate

## Pré-requisitos

- Exercícios 01 a 05 concluídos (você já sabe implementar cada check individualmente)
- Ambiente Docker rodando (Spark + Jupyter)
- Dataset `dados_sujos/vendas_problemas.parquet` disponível
- Tabelas de referência (clientes, produtos) disponíveis

## Duração Estimada

⏱️ ~20 minutos

---

## O Desafio

Construa a classe `DataQualityFramework` que encapsula toda a lógica de qualidade de dados em uma interface limpa e reutilizável. O framework deve funcionar com **qualquer** DataFrame — não apenas com o dataset de vendas.

### Interface Esperada (Assinaturas dos Métodos)

```python
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import DataFrame, SparkSession

@dataclass
class CheckResult:
    check_name: str          # Ex: "completeness_order_id"
    passed: bool             # True se passou no threshold
    metric_value: float      # Ex: 0.97 (97% completo)
    threshold: float         # Ex: 0.95
    details: Dict[str, Any]  # Detalhes específicos do check
    severity: str            # "critical" | "warning" | "info"


class DataQualityFramework:
    def __init__(self, spark: SparkSession):
        """Inicializa o framework com a SparkSession."""
        ...

    def check_completeness(
        self, df: DataFrame, columns: List[str], threshold: float = 0.95
    ) -> CheckResult:
        """
        Verifica completude (% não-nulo) das colunas especificadas.
        Retorna CheckResult com metric_value = menor taxa de preenchimento entre as colunas.
        """
        ...

    def check_uniqueness(
        self, df: DataFrame, key_columns: List[str]
    ) -> CheckResult:
        """
        Verifica unicidade das key_columns (sem duplicatas).
        Retorna CheckResult com metric_value = % de registros únicos.
        """
        ...

    def check_referential_integrity(
        self, df_source: DataFrame, df_reference: DataFrame,
        source_col: str, ref_col: str
    ) -> CheckResult:
        """
        Verifica se todos os valores de source_col existem em df_reference[ref_col].
        Retorna CheckResult com metric_value = % de registros com referência válida.
        """
        ...

    def check_validity(
        self, df: DataFrame, rules: Dict[str, str]
    ) -> CheckResult:
        """
        Verifica regras de domínio definidas como expressões SQL.
        rules = {"quantity_positive": "quantity > 0", "price_valid": "unit_price > 0"}
        Retorna CheckResult com metric_value = % de registros que passam em TODAS as regras.
        """
        ...

    def quarantine(
        self, df: DataFrame, rules: Dict[str, str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Separa o DataFrame em (df_valid, df_quarantine) com base nas regras.
        df_quarantine inclui coluna 'quarantine_reasons' com motivos da falha.
        """
        ...

    def run_all_checks(self, df: DataFrame, config: Dict[str, Any]) -> List[CheckResult]:
        """
        Executa todos os checks definidos no dicionário de configuração.
        config = {
            "completeness": {"columns": [...], "threshold": 0.95},
            "uniqueness": {"key_columns": [...]},
            "validity": {"rules": {...}},
            "referential_integrity": {...}
        }
        """
        ...

    def generate_report(self) -> Dict[str, Any]:
        """
        Gera relatório consolidado de todos os checks executados.
        Retorna dict com: checks_total, checks_passed, checks_failed,
        results (lista), gate_passed (bool), overall_score.
        """
        ...
```

### Uso Esperado (Como o Framework Deve Funcionar)

```python
# Instanciar
dq = DataQualityFramework(spark)

# Checks individuais
result1 = dq.check_completeness(df_vendas, ["order_id", "customer_id", "product_id"], threshold=0.95)
result2 = dq.check_uniqueness(df_vendas, ["order_id"])
result3 = dq.check_referential_integrity(df_vendas, df_clientes, "customer_id", "customer_id")
result4 = dq.check_validity(df_vendas, {
    "quantity_positive": "quantity > 0",
    "price_valid": "unit_price > 0",
    "date_not_future": "order_date <= current_date()"
})

# Quarentena
df_valid, df_quarantine = dq.quarantine(df_vendas, {
    "nulls": "order_id IS NOT NULL AND customer_id IS NOT NULL",
    "positive_values": "quantity > 0 AND unit_price > 0"
})

# Relatório consolidado
report = dq.generate_report()
print(f"Score geral: {report['overall_score']:.1%}")
print(f"Gate: {'✅ PASSED' if report['gate_passed'] else '❌ FAILED'}")

# Quality gate
if not report["gate_passed"]:
    raise Exception(f"Quality gate falhou! Score: {report['overall_score']:.1%}")
```

### Uso via Configuração (Batch)

```python
config = {
    "completeness": {
        "columns": ["order_id", "customer_id", "product_id", "order_date"],
        "threshold": 0.95
    },
    "uniqueness": {
        "key_columns": ["order_id"]
    },
    "validity": {
        "rules": {
            "quantity_positive": "quantity > 0",
            "price_valid": "unit_price > 0",
            "amount_valid": "total_amount > 0"
        }
    }
}

results = dq.run_all_checks(df_vendas, config)
report = dq.generate_report()
```

---

## Comportamento Esperado

Ao executar sua implementação com o dataset `vendas_problemas.parquet`:

```
🏗️ DataQualityFramework — DataFlow Analytics
================================================================

📋 Check 1: Completeness
   Colunas: ['order_id', 'customer_id', 'product_id', 'order_date']
   Menor completude: ~95.1%
   Threshold: 95.0%
   Resultado: ✅ PASSED (severity: critical)

📋 Check 2: Uniqueness
   Chaves: ['order_id']
   Unicidade: ~97.1%
   Resultado: ✅ PASSED (severity: critical)

📋 Check 3: Referential Integrity
   customer_id → clientes_referencia
   Integridade: ~95.3%
   Resultado: ⚠️ depende do threshold configurado

📋 Check 4: Validity
   Regras: quantity > 0, unit_price > 0, total_amount > 0
   Validade: ~95.1%
   Resultado: ✅ PASSED (severity: warning)

================================================================
📊 RELATÓRIO CONSOLIDADO
   Total de checks: 4
   Passed: 3-4
   Failed: 0-1
   Overall Score: ~95%
   Gate: ✅ PASSED (se threshold geral ≥ 90%)
================================================================
```

---

## Requisitos do Framework

| # | Requisito | Descrição |
|---|-----------|-----------|
| 1 | Cada check retorna `CheckResult` | Todos os métodos `check_*` devem retornar um `CheckResult` preenchido |
| 2 | Histórico interno de checks | O framework armazena todos os `CheckResult` para `generate_report()` |
| 3 | Quality gate configurável | `generate_report()` retorna `gate_passed` baseado em: TODOS os checks critical passaram |
| 4 | `run_all_checks` aceita config dict | Deve instanciar e executar checks a partir de um dicionário de configuração |
| 5 | Quarentena retorna tupla | `quarantine()` retorna `(df_valid, df_quarantine)` — sem perder registros |
| 6 | Regras de validade como expressões SQL | `check_validity` aceita `{"nome_regra": "expressão SQL"}` usando `expr()` do PySpark |
| 7 | Severidade impacta o gate | Checks com severity "critical" que falham → gate falha. "warning" apenas reporta |
| 8 | Framework genérico | Deve funcionar com QUALQUER DataFrame, não apenas vendas |

---

## Critérios de Validação

Seu exercício está completo quando:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | Classe `DataQualityFramework` instancia sem erros | `dq = DataQualityFramework(spark)` não levanta exceção |
| 2 | `check_completeness` retorna `CheckResult` válido | `result.passed` é bool, `result.metric_value` entre 0 e 1 |
| 3 | `check_uniqueness` detecta duplicatas corretamente | Com dataset sujo, `result.metric_value < 1.0` |
| 4 | `check_referential_integrity` identifica órfãos | Com chave inexistente na referência, `result.passed == False` |
| 5 | `check_validity` avalia expressões SQL | `{"positivo": "quantity > 0"}` filtra registros com quantity ≤ 0 |
| 6 | `quarantine` preserva todos os registros | `df_valid.count() + df_quarantine.count() == df.count()` |
| 7 | `run_all_checks` executa a partir de config | Passa config dict e recebe lista de `CheckResult` |
| 8 | `generate_report` retorna dict completo | Contém `gate_passed`, `overall_score`, `checks_total`, `results` |
| 9 | Quality gate falha quando critical check falha | Se `check_completeness` falha com severity "critical", `gate_passed == False` |
| 10 | Framework funciona com outro DataFrame | Testar com um DataFrame genérico (não vendas) — funciona igual |

---

## Dicas

<details>
<summary>💡 Dica 1 — Como armazenar histórico de checks na classe</summary>

Use uma lista interna para acumular resultados:

```python
class DataQualityFramework:
    def __init__(self, spark):
        self.spark = spark
        self._results: List[CheckResult] = []
    
    def _register(self, result: CheckResult) -> CheckResult:
        """Registra o resultado e retorna para o chamador."""
        self._results.append(result)
        return result
```

Cada método `check_*` chama `self._register(result)` antes de retornar.

</details>

<details>
<summary>💡 Dica 2 — Como avaliar regras de validade dinâmicas com expr()</summary>

O PySpark aceita expressões SQL como strings via `pyspark.sql.functions.expr`:

```python
from pyspark.sql.functions import expr

# Para verificar quantos registros passam em UMA regra:
passam = df.filter(expr(regra_sql)).count()
```

Para verificar TODAS as regras combinadas, use `&` (AND) entre os filtros.

</details>

<details>
<summary>💡 Dica 3 — Como calcular o overall_score e gate_passed</summary>

Uma abordagem simples para o score e gate:

```python
# overall_score = média dos metric_value de todos os checks
# gate_passed = todos os checks com severity "critical" passaram

critical_checks = [r for r in self._results if r.severity == "critical"]
gate_passed = all(r.passed for r in critical_checks)
```

</details>

<details>
<summary>💡 Dica 4 — Como implementar quarantine com múltiplas regras</summary>

Construa uma condição composta de "registro válido" (passa em TODAS as regras):

```python
from pyspark.sql.functions import expr, when, lit, concat_ws
from functools import reduce

# Um registro é válido se passa em TODAS as regras
# Um registro vai para quarentena se falha em QUALQUER regra
```

Para capturar os motivos, adicione uma coluna com os nomes das regras violadas antes de filtrar.

</details>

---

## Teste seu Framework

Para validar que o framework está genérico e reutilizável, teste com um DataFrame completamente diferente:

```python
# Criar DataFrame de teste simples
df_teste = spark.createDataFrame([
    (1, "Alice", 25, "SP"),
    (2, None, 30, "RJ"),      # nome nulo
    (3, "Carlos", -1, "MG"),  # idade inválida
    (1, "Alice", 25, "SP"),   # duplicata
], ["id", "nome", "idade", "estado"])

# Testar com o framework
dq2 = DataQualityFramework(spark)
dq2.check_completeness(df_teste, ["id", "nome"], threshold=0.90)
dq2.check_uniqueness(df_teste, ["id"])
dq2.check_validity(df_teste, {"idade_valida": "idade > 0"})

report = dq2.generate_report()
print(report)
# Deve mostrar: completeness ~75%, uniqueness ~75%, validity ~75%
```

---

## Perguntas para Reflexão

1. **Extensibilidade**: como você adicionaria um novo tipo de check (ex: `check_freshness` para verificar se dados são recentes) sem alterar o código existente?
2. **Performance**: o framework roda cada check separadamente. Seria possível combinar checks em uma única passada pelo DataFrame? Qual o trade-off?
3. **Configuração externa**: em produção, o `config` dict viria de um arquivo YAML/JSON. Como isso facilitaria a governança de dados entre equipes?
4. **Integração com Airflow**: como você usaria este framework dentro de uma task Airflow? O `generate_report()` retornaria via XCom?
5. **Imutabilidade**: cada chamada de `check_*` acumula no `_results`. Como você implementaria um `reset()` para reusar a mesma instância em múltiplos DataFrames?

---

## Próximos Passos

Parabéns! Ao completar este desafio, você construiu um componente de engenharia de dados real e reutilizável. Na **Aula 07**, vamos integrar este framework em um pipeline end-to-end de produção: containerizado, orquestrado pelo Airflow, com logging estruturado e escrita idempotente.

> **Marina Silva:** "Este framework é exatamente o que eu queria. Agora qualquer novo pipeline na DataFlow começa com 5 linhas de quality check. Sem desculpas para dados sujos chegarem no Gold. Carlos, faça o onboarding da equipe na próxima sprint."
