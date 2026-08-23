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
