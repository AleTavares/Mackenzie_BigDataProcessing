# Exercício 2 — Implementar Logging Estruturado no Spark Job

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, nosso pipeline está rodando, mas os logs estão em texto livre. Quando algo falha às 3 da manhã, o pessoal de SRE demora 20 minutos filtrando linhas no terminal. Precisamos de logs em JSON — o ELK e o CloudWatch conseguem parsear, indexar e alertar automaticamente."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Faz total sentido. Logs em JSON permitem filtrar por `stage=silver`, buscar por `level=ERROR`, e criar alertas quando `duration_seconds > 60`. Vou implementar um `JsonFormatter` e adicionar campos contextuais em cada entrada de log."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender por que logs JSON são superiores a texto livre em produção
- Criar uma classe `JsonFormatter` para emitir logs estruturados
- Adicionar campos contextuais: `pipeline_name`, `data_ref`, `stage`, `duration_seconds`
- Usar níveis de log adequados (INFO, WARNING, ERROR)
- Criar um decorator para medir duração de cada etapa
- Emitir entradas de log do tipo métrica (`type="metric"`) para dashboards

## Pré-requisitos

- Exercício 01 concluído (`pipeline_vendas.py` com CLI funcionando)
- Ambiente Docker rodando (Spark + Jupyter)
- Terminal com acesso ao container Spark

## Por que Logging Estruturado?

O `pipeline_vendas.py` do Exercício 01 usa logs em texto livre:

```
[2024-01-15 06:03:22] INFO | pipeline_vendas | [BRONZE] Registros lidos: 52,340
```

O problema? Ferramentas de monitoramento (ELK, Splunk, CloudWatch) **não conseguem parsear** texto livre de forma confiável. Comparação:

| Aspecto | Texto Livre | JSON Estruturado |
|---------|-------------|-----------------|
| Busca por campo | `grep` com regex frágil | `jq '.stage == "bronze"'` |
| Alertas automáticos | Impossível sem regex | `duration_seconds > 60` → alerta |
| Dashboards | Manual | Agregação automática por campo |
| Correlação | Difícil | Filtrar por `data_ref` + `pipeline` |
| Parsing | Quebra se formato mudar | Contrato estável (JSON schema) |

> **Marina:** "Com JSON, o CloudWatch consegue criar um alerta automático: se `level=ERROR` ou se `duration_seconds` de qualquer etapa exceder 2x a média histórica, o time recebe notificação no Slack."

---

## Passo 1: Criar a Classe JsonFormatter

**Descrição:** Vamos substituir o formatter de texto livre por um que emite JSON. A classe herda de `logging.Formatter` e sobrescreve o método `format()`.

**Crie o arquivo** `aula_07/code/spark_jobs/structured_logging.py`:

```bash
cat > aula_07/code/spark_jobs/structured_logging.py << 'EOF'
"""
Módulo de Logging Estruturado — DataFlow Analytics
====================================================
Fornece logging em formato JSON para pipelines de produção.
Logs estruturados permitem busca, filtragem e alertas automáticos
em ferramentas como ELK, Splunk e CloudWatch.

Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Versão: 1.0.0
"""

import json
import logging
import time
import sys
from datetime import datetime, timezone
from functools import wraps


class JsonFormatter(logging.Formatter):
    """
    Formatter que emite logs em formato JSON.

    Cada linha de log é um objeto JSON válido com campos padronizados,
    permitindo parsing automático por ferramentas de monitoramento.
    """

    def __init__(self, pipeline_name: str, data_ref: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.data_ref = data_ref

    def format(self, record: logging.LogRecord) -> str:
        """Formata um LogRecord como JSON de uma linha."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_name": self.pipeline_name,
            "data_ref": self.data_ref,
            "module": record.module,
            "function": record.funcName,
        }

        # Adiciona campos extras se existirem no record
        if hasattr(record, "stage"):
            log_entry["stage"] = record.stage
        if hasattr(record, "duration_seconds"):
            log_entry["duration_seconds"] = record.duration_seconds
        if hasattr(record, "records_count"):
            log_entry["records_count"] = record.records_count
        if hasattr(record, "log_type"):
            log_entry["type"] = record.log_type

        return json.dumps(log_entry, ensure_ascii=False)
EOF
```

**O que cada campo significa:**

| Campo | Descrição | Exemplo |
|-------|-----------|---------|
| `timestamp` | Momento exato em UTC (ISO 8601) | `"2024-01-15T09:03:22.456Z"` |
| `level` | Severidade do evento | `"INFO"`, `"ERROR"` |
| `pipeline_name` | Identifica qual pipeline gerou o log | `"vendas_producao"` |
| `data_ref` | Data sendo processada | `"2024-01-15"` |
| `stage` | Etapa do pipeline (Bronze/Silver/Gold) | `"bronze"` |
| `duration_seconds` | Quanto tempo a operação levou | `12.4` |
| `records_count` | Quantos registros foram processados | `52340` |
| `type` | Tipo de entrada (log ou metric) | `"metric"` |

> **Carlos:** "O segredo é que cada linha de log é **um JSON válido**. Ferramentas como `jq` no terminal, ou ingestores como Fluentd e Logstash, conseguem parsear instantaneamente."

---

## Passo 2: Criar a Função de Setup do Logger

**Descrição:** Agora criamos a função que configura o logger com nosso `JsonFormatter`. Ela substitui o `configurar_logging()` do Exercício 01.

**Adicione ao final do arquivo** `structured_logging.py`:

```python
def setup_structured_logger(
    name: str,
    pipeline_name: str,
    data_ref: str,
    log_level: str = "INFO"
) -> logging.Logger:
    """
    Configura logger com saída JSON estruturada.

    Args:
        name: Nome do logger (ex: "pipeline_vendas")
        pipeline_name: Identificador do pipeline (ex: "vendas_producao")
        data_ref: Data de referência sendo processada
        log_level: Nível mínimo de log (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Logger configurado com JsonFormatter
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Evitar duplicação de handlers se chamado múltiplas vezes
    if logger.handlers:
        logger.handlers.clear()

    # Handler para stdout (capturado pelo Docker/CloudWatch)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(pipeline_name, data_ref))
    logger.addHandler(handler)

    return logger
```

**Por que `sys.stdout`?** Em containers Docker, tudo que vai para `stdout` é automaticamente capturado pelo runtime (Docker logs, CloudWatch, Kubernetes). Usar `stderr` ou arquivos exige configuração extra.

---

## Passo 3: Criar o Decorator de Timing

**Descrição:** Um decorator que mede a duração de cada etapa do pipeline e emite um log com `duration_seconds`. Isso permite criar alertas se uma etapa demorar mais que o esperado.

**Adicione ao final do arquivo** `structured_logging.py`:

```python
def log_stage_duration(logger: logging.Logger, stage: str):
    """
    Decorator que mede e loga a duração de uma etapa do pipeline.

    Emite logs de INFO no início e fim, com duration_seconds no final.
    Em caso de erro, emite log de ERROR com a exceção.

    Args:
        logger: Logger configurado
        stage: Nome da etapa (bronze, silver, gold)

    Uso:
        @log_stage_duration(logger, "bronze")
        def etapa_bronze(spark, ...):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Log de início
            logger.info(
                f"Iniciando etapa {stage}",
                extra={"stage": stage}
            )

            inicio = time.time()

            try:
                resultado = func(*args, **kwargs)
                duracao = time.time() - inicio

                # Log de conclusão com duração
                logger.info(
                    f"Etapa {stage} concluída em {duracao:.2f}s",
                    extra={
                        "stage": stage,
                        "duration_seconds": round(duracao, 2)
                    }
                )

                return resultado

            except Exception as e:
                duracao = time.time() - inicio
                logger.error(
                    f"Erro na etapa {stage} após {duracao:.2f}s: {e}",
                    extra={
                        "stage": stage,
                        "duration_seconds": round(duracao, 2)
                    }
                )
                raise  # Re-raise para o tratamento no main()

        return wrapper
    return decorator
```

**Como funciona o decorator:**

```
┌──────────────────────────────────────────────────────────────┐
│  @log_stage_duration(logger, "bronze")                        │
│  def etapa_bronze(spark, ...):                                │
│                                                               │
│  Ao chamar etapa_bronze():                                    │
│    1. Emite: {"stage":"bronze", "message":"Iniciando..."}     │
│    2. Mede: inicio = time.time()                              │
│    3. Executa: resultado = etapa_bronze_original(...)          │
│    4. Calcula: duracao = time.time() - inicio                 │
│    5. Emite: {"stage":"bronze", "duration_seconds": 12.4}     │
│    6. Retorna: resultado                                      │
│                                                               │
│  Se der erro:                                                 │
│    4. Emite: {"level":"ERROR", "duration_seconds": 3.1}       │
│    5. Re-raise: a exceção sobe para o main()                  │
└──────────────────────────────────────────────────────────────┘
```

---

## Passo 4: Criar a Função de Métricas

**Descrição:** Além de logs de evento, emitimos entradas do tipo `metric` — valores numéricos que ferramentas de monitoramento podem agregar e plotar em dashboards.

**Adicione ao final do arquivo** `structured_logging.py`:

```python
def emitir_metrica(
    logger: logging.Logger,
    stage: str,
    records_count: int,
    **extras
):
    """
    Emite uma entrada de log do tipo métrica.

    Métricas são diferentes de logs de evento — representam valores numéricos
    que podem ser agregados em dashboards (Grafana, CloudWatch Metrics).

    Args:
        logger: Logger configurado
        stage: Etapa que gerou a métrica (bronze, silver, gold)
        records_count: Número de registros processados
        **extras: Campos adicionais (ex: records_rejected=150)

    Saída JSON:
        {"type":"metric", "stage":"bronze", "records_count":52340, ...}
    """
    extra_fields = {
        "log_type": "metric",
        "stage": stage,
        "records_count": records_count,
    }
    extra_fields.update(extras)

    logger.info(
        f"Métrica {stage}: {records_count} registros",
        extra=extra_fields
    )
```

**Diferença entre log de evento e métrica:**

| Tipo | Propósito | Exemplo |
|------|-----------|---------|
| **Evento** (log) | Registrar o que aconteceu | `"Iniciando etapa bronze"` |
| **Métrica** (metric) | Valor numérico para dashboard | `records_count=52340` |

```json
// Evento (type omitido ou "event")
{"level":"INFO", "message":"Etapa bronze concluída em 12.4s", "stage":"bronze"}

// Métrica (type="metric")
{"type":"metric", "stage":"bronze", "records_count":52340, "records_rejected":0}
```

> **Marina:** "As métricas são o que alimenta nossos dashboards no Grafana. Se o `records_count` cair 80% de um dia pro outro, algo está errado com os dados de entrada — mesmo sem erros no pipeline."

---

## Passo 5: Arquivo Completo — structured_logging.py

**Descrição:** Aqui está o módulo completo. Crie-o de uma vez:

```bash
cat > aula_07/code/spark_jobs/structured_logging.py << 'EOF'
"""
Módulo de Logging Estruturado — DataFlow Analytics
====================================================
Fornece logging em formato JSON para pipelines de produção.
Logs estruturados permitem busca, filtragem e alertas automáticos
em ferramentas como ELK, Splunk e CloudWatch.

Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Versão: 1.0.0
"""

import json
import logging
import time
import sys
from datetime import datetime, timezone
from functools import wraps


class JsonFormatter(logging.Formatter):
    """
    Formatter que emite logs em formato JSON.

    Cada linha de log é um objeto JSON válido com campos padronizados,
    permitindo parsing automático por ferramentas de monitoramento.
    """

    def __init__(self, pipeline_name: str, data_ref: str):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.data_ref = data_ref

    def format(self, record: logging.LogRecord) -> str:
        """Formata um LogRecord como JSON de uma linha."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_name": self.pipeline_name,
            "data_ref": self.data_ref,
            "module": record.module,
            "function": record.funcName,
        }

        # Campos extras opcionais
        if hasattr(record, "stage"):
            log_entry["stage"] = record.stage
        if hasattr(record, "duration_seconds"):
            log_entry["duration_seconds"] = record.duration_seconds
        if hasattr(record, "records_count"):
            log_entry["records_count"] = record.records_count
        if hasattr(record, "log_type"):
            log_entry["type"] = record.log_type

        return json.dumps(log_entry, ensure_ascii=False)


def setup_structured_logger(
    name: str,
    pipeline_name: str,
    data_ref: str,
    log_level: str = "INFO"
) -> logging.Logger:
    """
    Configura logger com saída JSON estruturada.

    Args:
        name: Nome do logger (ex: "pipeline_vendas")
        pipeline_name: Identificador do pipeline (ex: "vendas_producao")
        data_ref: Data de referência sendo processada
        log_level: Nível mínimo de log (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Logger configurado com JsonFormatter
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Evitar duplicação de handlers
    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(pipeline_name, data_ref))
    logger.addHandler(handler)

    return logger


def log_stage_duration(logger: logging.Logger, stage: str):
    """
    Decorator que mede e loga a duração de uma etapa do pipeline.

    Emite logs no início e fim, com duration_seconds.
    Em caso de erro, emite ERROR com a exceção.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(
                f"Iniciando etapa {stage}",
                extra={"stage": stage}
            )

            inicio = time.time()

            try:
                resultado = func(*args, **kwargs)
                duracao = time.time() - inicio

                logger.info(
                    f"Etapa {stage} concluída em {duracao:.2f}s",
                    extra={
                        "stage": stage,
                        "duration_seconds": round(duracao, 2)
                    }
                )
                return resultado

            except Exception as e:
                duracao = time.time() - inicio
                logger.error(
                    f"Erro na etapa {stage} após {duracao:.2f}s: {e}",
                    extra={
                        "stage": stage,
                        "duration_seconds": round(duracao, 2)
                    }
                )
                raise

        return wrapper
    return decorator


def emitir_metrica(
    logger: logging.Logger,
    stage: str,
    records_count: int,
    **extras
):
    """
    Emite uma entrada de log do tipo métrica para dashboards.

    Args:
        logger: Logger configurado
        stage: Etapa que gerou a métrica
        records_count: Número de registros processados
        **extras: Campos adicionais (records_rejected, etc.)
    """
    extra_fields = {
        "log_type": "metric",
        "stage": stage,
        "records_count": records_count,
    }
    extra_fields.update(extras)

    logger.info(
        f"Métrica {stage}: {records_count} registros",
        extra=extra_fields
    )
EOF
```

---

## Passo 6: Integrar no pipeline_vendas.py

**Descrição:** Agora vamos atualizar o `pipeline_vendas.py` para usar o novo módulo de logging estruturado. As mudanças são mínimas — substituímos o logger e adicionamos métricas.

**Edite o início do arquivo** `aula_07/code/spark_jobs/pipeline_vendas.py`:

Substitua a seção de imports e logging:

```python
# ANTES (texto livre):
# from logging import ...
# def configurar_logging(log_level):
#     formatter = logging.Formatter("[%(asctime)s] ...")

# DEPOIS (JSON estruturado):
from structured_logging import (
    setup_structured_logger,
    log_stage_duration,
    emitir_metrica,
)
```

**Alterações na função `main()`:**

```python
def main():
    args = parse_args()

    # ✅ NOVO: Logging estruturado em JSON
    logger = setup_structured_logger(
        name="pipeline_vendas",
        pipeline_name="vendas_producao",
        data_ref=args.data_ref,
        log_level=args.log_level,
    )

    logger.info("Pipeline iniciado", extra={"stage": "init"})

    spark = None

    try:
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada — versão {spark.version}")

        # Etapa Bronze
        registros_bronze = etapa_bronze(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )
        # ✅ NOVO: Emitir métrica
        emitir_metrica(logger, "bronze", registros_bronze)

        # Etapa Silver
        registros_silver = etapa_silver(
            spark, args.output_path, args.data_ref, logger
        )
        emitir_metrica(
            logger, "silver", registros_silver,
            records_rejected=registros_bronze - registros_silver
        )

        # Etapa Gold
        registros_gold = etapa_gold(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )
        emitir_metrica(logger, "gold", registros_gold)

        # ✅ NOVO: Métrica de resumo
        logger.info("Pipeline concluído com sucesso", extra={"stage": "done"})

    except Exception as e:
        logger.error(
            f"Pipeline falhou: {e}",
            extra={"stage": "error"}
        )
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
```

---

## Passo 7: Usar o Decorator nas Etapas

**Descrição:** Opcionalmente, podemos usar o decorator `@log_stage_duration` para medir automaticamente o tempo de cada etapa, sem alterar o corpo das funções.

**Exemplo de uso no pipeline:**

```python
# Após criar o logger no main(), decore as funções:

@log_stage_duration(logger, "bronze")
def etapa_bronze(spark, input_path, output_path, data_ref, logger):
    # ... código existente sem alteração ...
    return contagem

@log_stage_duration(logger, "silver")
def etapa_silver(spark, output_path, data_ref, logger):
    # ... código existente sem alteração ...
    return contagem_saida

@log_stage_duration(logger, "gold")
def etapa_gold(spark, input_path, output_path, data_ref, logger):
    # ... código existente sem alteração ...
    return contagem_gold
```

> **Nota:** Como o decorator precisa do `logger` que é criado em runtime, uma alternativa é aplicá-lo dentro do `main()` usando chamada direta:

```python
# Alternativa: aplicar decorator em runtime
etapa_bronze_com_log = log_stage_duration(logger, "bronze")(etapa_bronze)
registros_bronze = etapa_bronze_com_log(spark, args.input_path, ...)
```

---

## Passo 8: Testar a Saída JSON

**Descrição:** Vamos executar um teste rápido para ver os logs JSON sendo emitidos. Mesmo sem dados reais, podemos testar o módulo isoladamente.

**Crie um script de teste:**

```bash
cat > aula_07/code/spark_jobs/test_logging.py << 'EOF'
"""Teste rápido do módulo de logging estruturado."""
from structured_logging import (
    setup_structured_logger,
    log_stage_duration,
    emitir_metrica,
)
import time

# Configurar logger
logger = setup_structured_logger(
    name="test",
    pipeline_name="vendas_producao",
    data_ref="2024-01-15",
    log_level="DEBUG",
)

# Testar diferentes níveis
logger.info("Pipeline iniciado", extra={"stage": "init"})
logger.warning("Volume abaixo do esperado", extra={"stage": "bronze"})
logger.error("Falha na conexão com storage", extra={"stage": "silver"})

# Testar decorator
@log_stage_duration(logger, "bronze")
def simular_bronze():
    time.sleep(0.5)  # Simula processamento
    return 52340

registros = simular_bronze()

# Testar métricas
emitir_metrica(logger, "bronze", registros)
emitir_metrica(logger, "silver", 51200, records_rejected=1140)

print("\n✅ Todos os logs emitidos com sucesso!")
print("Use 'python test_logging.py | jq .' para ver formatado")
EOF
```

**Execute o teste:**

```bash
cd aula_07/code/spark_jobs
python test_logging.py
```

**Saída esperada (cada linha é um JSON válido):**

```json
{"timestamp": "2024-01-15T09:03:22.123456+00:00", "level": "INFO", "logger": "test", "message": "Pipeline iniciado", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "<module>", "stage": "init"}
{"timestamp": "2024-01-15T09:03:22.123789+00:00", "level": "WARNING", "logger": "test", "message": "Volume abaixo do esperado", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "<module>", "stage": "bronze"}
{"timestamp": "2024-01-15T09:03:22.124000+00:00", "level": "ERROR", "logger": "test", "message": "Falha na conexão com storage", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "<module>", "stage": "silver"}
{"timestamp": "2024-01-15T09:03:22.124100+00:00", "level": "INFO", "logger": "test", "message": "Iniciando etapa bronze", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "wrapper", "stage": "bronze"}
{"timestamp": "2024-01-15T09:03:22.624200+00:00", "level": "INFO", "logger": "test", "message": "Etapa bronze concluída em 0.50s", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "wrapper", "stage": "bronze", "duration_seconds": 0.5}
{"timestamp": "2024-01-15T09:03:22.624300+00:00", "level": "INFO", "logger": "test", "message": "Métrica bronze: 52340 registros", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "emitir_metrica", "stage": "bronze", "records_count": 52340, "type": "metric"}
{"timestamp": "2024-01-15T09:03:22.624400+00:00", "level": "INFO", "logger": "test", "message": "Métrica silver: 51200 registros", "pipeline_name": "vendas_producao", "data_ref": "2024-01-15", "module": "test_logging", "function": "emitir_metrica", "stage": "silver", "records_count": 51200, "type": "metric"}
```

**Para visualizar formatado (instale `jq` se necessário):**

```bash
python test_logging.py 2>/dev/null | head -1 | jq .
```

```json
{
  "timestamp": "2024-01-15T09:03:22.123456+00:00",
  "level": "INFO",
  "logger": "test",
  "message": "Pipeline iniciado",
  "pipeline_name": "vendas_producao",
  "data_ref": "2024-01-15",
  "module": "test_logging",
  "function": "<module>",
  "stage": "init"
}
```

---

## Passo 9: Como Ferramentas de Monitoramento Consomem os Logs

**Descrição:** Com logs em JSON, ferramentas de observabilidade podem fazer busca, filtragem e alertas automaticamente. Vamos ver exemplos reais de como isso funciona.

**Exemplos de queries com os logs JSON:**

```bash
# 1. Filtrar só erros do pipeline de vendas
cat logs.jsonl | jq 'select(.level == "ERROR")'

# 2. Ver métricas da etapa Silver
cat logs.jsonl | jq 'select(.type == "metric" and .stage == "silver")'

# 3. Encontrar etapas que demoraram mais de 30 segundos
cat logs.jsonl | jq 'select(.duration_seconds > 30)'

# 4. Buscar logs de uma data específica
cat logs.jsonl | jq 'select(.data_ref == "2024-01-15")'

# 5. Contar registros rejeitados por dia (para dashboard)
cat logs.jsonl | jq 'select(.type == "metric" and .stage == "silver") | .records_rejected'
```

**Em ferramentas reais:**

| Ferramenta | Como usa os logs JSON |
|------------|----------------------|
| **CloudWatch Logs Insights** | `fields @timestamp, message \| filter level = "ERROR"` |
| **Elasticsearch/Kibana** | Index automático por campo, dashboards visuais |
| **Splunk** | `index=pipeline level=ERROR stage=silver` |
| **Grafana Loki** | `{pipeline="vendas_producao"} \|= "ERROR"` |
| **DataDog** | Parsing automático, alertas por threshold |

**Fluxo completo em produção:**

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Spark Job   │────▶│   stdout     │────▶│  Docker logs │
│  (JSON logs) │     │  (container) │     │  (driver)    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                                                  ▼
                                         ┌──────────────┐
                                         │  Fluentd /   │
                                         │  CloudWatch  │
                                         │  Agent       │
                                         └──────┬───────┘
                                                │
                                    ┌───────────┼───────────┐
                                    ▼           ▼           ▼
                              ┌──────────┐ ┌────────┐ ┌─────────┐
                              │Dashboards│ │Alertas │ │ Busca   │
                              │(Grafana) │ │(Slack) │ │(Kibana) │
                              └──────────┘ └────────┘ └─────────┘
```

> **Marina:** "Quando o pipeline roda no ECS ou Kubernetes, o Docker captura tudo que vai para stdout. O Fluentd/CloudWatch Agent pega esses logs e, como são JSON, indexa cada campo automaticamente. Zero configuração de parsing."

---

## Resumo

Neste exercício você aprendeu a:

| Antes (Ex. 01) | Depois (Ex. 02) |
|----------------|-----------------|
| `[2024-01-15] INFO \| mensagem` | `{"timestamp":"...", "level":"INFO", ...}` |
| Texto livre, impossível de filtrar | JSON parseável por qualquer ferramenta |
| Sem contexto | Campos: `pipeline_name`, `data_ref`, `stage` |
| Sem métricas | Entradas `type="metric"` para dashboards |
| Sem timing | `duration_seconds` em cada etapa |
| Debugging manual | Busca por campo, alertas automáticos |

**Estrutura final dos arquivos:**

```
aula_07/code/spark_jobs/
├── pipeline_vendas.py        ← atualizado com logging JSON
├── structured_logging.py     ← NOVO: módulo de logging estruturado
└── test_logging.py           ← NOVO: teste do módulo
```

**Conceitos-chave para lembrar:**

1. **JSON > texto livre** — ferramentas de monitoramento parseiam JSON nativamente
2. **Campos contextuais** — `pipeline_name`, `data_ref`, `stage` permitem filtro preciso
3. **Métricas vs eventos** — métricas são números agregáveis, eventos são acontecimentos
4. **Decorator de timing** — mede duração sem alterar o corpo da função
5. **stdout em containers** — Docker/K8s captura stdout automaticamente

---

## Próximo Passo

No **Exercício 03**, vamos implementar **escrita idempotente** — garantindo que executar o pipeline 2x com a mesma `data_ref` produz o mesmo resultado, sem duplicar dados.
