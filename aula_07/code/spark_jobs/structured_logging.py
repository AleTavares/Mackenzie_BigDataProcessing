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

    Métricas são diferentes de logs de evento — representam valores numéricos
    que podem ser agregados em dashboards (Grafana, CloudWatch Metrics).

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
