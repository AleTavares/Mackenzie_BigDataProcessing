#!/bin/bash
# =============================================================================
# start_env.sh - Iniciar ambiente de laboratório
# Curso: Big Data Processing - MBA Engenharia de Dados (Mackenzie)
# =============================================================================
# Script para iniciar os serviços Docker do ambiente de laboratório.
# Deve ser executado a partir da RAIZ do repositório.
#
# Uso:
#   ./shared/start_env.sh              # Ambiente base (Spark + Jupyter)
#   ./shared/start_env.sh base         # Ambiente base (Spark + Jupyter)
#   ./shared/start_env.sh airflow      # Base + Airflow
#   ./shared/start_env.sh full         # Stack completa (Spark + Airflow + Jupyter)
#   ./shared/start_env.sh --help       # Exibir ajuda
# =============================================================================
set -e

# Cores para mensagens no terminal
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # Sem cor

# ---------------------------------------------------------------------------
# Função: exibir mensagem de ajuda
# ---------------------------------------------------------------------------
mostrar_ajuda() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  🚀 Big Data Processing - Iniciar Ambiente de Laboratório  ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Uso: ./shared/start_env.sh [AMBIENTE]"
    echo ""
    echo "Ambientes disponíveis:"
    echo "  base      (padrão) Jupyter Notebook com PySpark embutido (Spark local)"
    echo "            Indicado para: Aulas 1, 2 e 3"
    echo ""
    echo "  airflow   Base + Apache Airflow (Webserver + Scheduler)"
    echo "            Indicado para: Aulas 4 e 5"
    echo ""
    echo "  full      Stack completa: Spark cluster + Airflow + Jupyter"
    echo "            Indicado para: Aulas 6 e 7"
    echo ""
    echo "Exemplos:"
    echo "  ./shared/start_env.sh              # Inicia ambiente base"
    echo "  ./shared/start_env.sh airflow      # Inicia com Airflow"
    echo "  ./shared/start_env.sh full         # Inicia stack completa"
    echo ""
    echo "Portas dos serviços:"
    echo "  Jupyter Notebook:  http://localhost:8888"
    echo "  Spark App UI:      http://localhost:4040  (ativa durante execução de jobs)"
    echo "  Spark Master UI:   http://localhost:8080  (apenas nos ambientes airflow/full)"
    echo "  Airflow Webserver: http://localhost:8081  (login: admin/admin, apenas airflow/full)"
    echo ""
}

# ---------------------------------------------------------------------------
# Verificar se Docker está disponível
# ---------------------------------------------------------------------------
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}❌ Erro: Docker não encontrado. Instale o Docker antes de continuar.${NC}"
    exit 1
fi

if ! docker info &> /dev/null 2>&1; then
    echo -e "${YELLOW}❌ Erro: Docker daemon não está rodando. Inicie o Docker e tente novamente.${NC}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Processar argumento (ambiente desejado)
# ---------------------------------------------------------------------------
AMBIENTE="${1:-base}"

case "$AMBIENTE" in
    --help|-h)
        mostrar_ajuda
        exit 0
        ;;
    base)
        echo ""
        echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
        echo -e "${BLUE}║  🚀 Iniciando ambiente BASE (Jupyter + PySpark local)   ║${NC}"
        echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo -e "${GREEN}Serviços:${NC}"
        echo "  • Jupyter Notebook com PySpark embutido (Spark local mode)"
        echo ""

        docker compose -f shared/docker-compose.yml up -d

        echo ""
        echo -e "${GREEN}✅ Ambiente base iniciado com sucesso!${NC}"
        echo ""
        echo "Acesse os serviços:"
        echo -e "  Jupyter Notebook: ${BLUE}http://localhost:8888${NC}"
        echo -e "  Spark App UI:     ${BLUE}http://localhost:4040${NC}  (ativa enquanto um SparkSession estiver rodando)"
        echo ""
        echo -e "${YELLOW}Nota:${NC} Neste ambiente o Spark roda em modo local (local[*]) dentro do container."
        echo "      Não há Spark Master UI separada. A porta 4040 só aparece durante a execução de jobs."
        echo ""
        echo "Dica: use 'docker compose -f shared/docker-compose.yml logs -f' para ver os logs."
        ;;
    airflow)
        echo ""
        echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
        echo -e "${BLUE}║  🚀 Iniciando ambiente AIRFLOW (Base + Orquestração)    ║${NC}"
        echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo -e "${GREEN}Serviços:${NC}"
        echo "  • Spark Master (cluster manager)"
        echo "  • Spark Worker (processamento)"
        echo "  • Jupyter Notebook (interface interativa)"
        echo "  • Airflow Webserver (interface de monitoramento)"
        echo "  • Airflow Scheduler (agendador de DAGs)"
        echo ""
        echo -e "${YELLOW}⏳ O Airflow pode levar ~60s para inicializar completamente...${NC}"
        echo ""

        docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d

        echo ""
        echo -e "${GREEN}✅ Ambiente com Airflow iniciado com sucesso!${NC}"
        echo ""
        echo "Acesse os serviços:"
        echo -e "  Spark Master UI:   ${BLUE}http://localhost:8080${NC}"
        echo -e "  Airflow Webserver: ${BLUE}http://localhost:8081${NC}  (login: admin/admin)"
        echo -e "  Jupyter Notebook:  ${BLUE}http://localhost:8888${NC}"
        echo ""
        echo "Dica: aguarde o healthcheck do Airflow antes de acessar a interface web."
        ;;
    full)
        echo ""
        echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
        echo -e "${BLUE}║  🚀 Iniciando ambiente FULL (Stack Completa)            ║${NC}"
        echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo -e "${GREEN}Serviços:${NC}"
        echo "  • Spark Master (cluster manager)"
        echo "  • Spark Worker (processamento)"
        echo "  • Jupyter Notebook (interface interativa)"
        echo "  • Airflow Webserver (interface de monitoramento)"
        echo "  • Airflow Scheduler (agendador de DAGs)"
        echo ""
        echo -e "${YELLOW}⏳ A stack completa pode levar ~90s para todos os serviços ficarem prontos...${NC}"
        echo ""

        docker compose -f shared/docker-compose.full.yml up -d

        echo ""
        echo -e "${GREEN}✅ Stack completa iniciada com sucesso!${NC}"
        echo ""
        echo "Acesse os serviços:"
        echo -e "  Spark Master UI:   ${BLUE}http://localhost:8080${NC}"
        echo -e "  Airflow Webserver: ${BLUE}http://localhost:8081${NC}  (login: admin/admin)"
        echo -e "  Jupyter Notebook:  ${BLUE}http://localhost:8888${NC}"
        echo ""
        echo "Dica: use 'docker compose -f shared/docker-compose.full.yml logs -f' para ver os logs."
        ;;
    *)
        echo -e "${YELLOW}❌ Erro: Ambiente '$AMBIENTE' não reconhecido.${NC}"
        echo ""
        echo "Ambientes válidos: base, airflow, full"
        echo "Use './shared/start_env.sh --help' para mais informações."
        exit 1
        ;;
esac
