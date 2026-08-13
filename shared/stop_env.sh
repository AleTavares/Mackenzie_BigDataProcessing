#!/bin/bash
# =============================================================================
# stop_env.sh - Parar ambiente de laboratório
# Curso: Big Data Processing - MBA Engenharia de Dados (Mackenzie)
# =============================================================================
# Script para parar todos os serviços Docker do ambiente de laboratório.
# Deve ser executado a partir da RAIZ do repositório.
#
# Uso:
#   ./shared/stop_env.sh              # Para todos os serviços
#   ./shared/stop_env.sh --volumes    # Para serviços e remove volumes (dados)
#   ./shared/stop_env.sh --help       # Exibir ajuda
# =============================================================================
set -e

# Cores para mensagens no terminal
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # Sem cor

# ---------------------------------------------------------------------------
# Função: exibir mensagem de ajuda
# ---------------------------------------------------------------------------
mostrar_ajuda() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  🛑 Big Data Processing - Parar Ambiente de Laboratório    ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Uso: ./shared/stop_env.sh [OPÇÕES]"
    echo ""
    echo "Opções:"
    echo "  --volumes, -v   Também remove volumes Docker (apaga dados do Airflow,"
    echo "                  banco SQLite, etc.) - pede confirmação antes de executar"
    echo "  --help, -h      Exibe esta mensagem de ajuda"
    echo ""
    echo "Exemplos:"
    echo "  ./shared/stop_env.sh              # Para serviços, mantém volumes"
    echo "  ./shared/stop_env.sh --volumes    # Para serviços e limpa volumes"
    echo ""
    echo "Nota: Este script para TODOS os ambientes (base, airflow, full)."
    echo ""
}

# ---------------------------------------------------------------------------
# Verificar se Docker está disponível
# ---------------------------------------------------------------------------
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}❌ Erro: Docker não encontrado.${NC}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Processar argumentos
# ---------------------------------------------------------------------------
REMOVER_VOLUMES=false

case "${1:-}" in
    --help|-h)
        mostrar_ajuda
        exit 0
        ;;
    --volumes|-v)
        REMOVER_VOLUMES=true
        ;;
    "")
        # Sem argumentos - comportamento padrão
        ;;
    *)
        echo -e "${YELLOW}❌ Erro: Opção '$1' não reconhecida.${NC}"
        echo "Use './shared/stop_env.sh --help' para ver as opções disponíveis."
        exit 1
        ;;
esac

# ---------------------------------------------------------------------------
# Confirmar remoção de volumes (se solicitado)
# ---------------------------------------------------------------------------
if [ "$REMOVER_VOLUMES" = true ]; then
    echo ""
    echo -e "${RED}⚠️  ATENÇÃO: Você solicitou remoção de volumes Docker.${NC}"
    echo ""
    echo "Isso irá apagar permanentemente:"
    echo "  • Banco de dados do Airflow (DAGs executadas, histórico)"
    echo "  • Configurações e usuários do Airflow"
    echo "  • Quaisquer dados persistidos em volumes Docker"
    echo ""
    read -p "Tem certeza que deseja continuar? (s/N): " confirmacao
    echo ""

    if [[ ! "$confirmacao" =~ ^[Ss]$ ]]; then
        echo -e "${BLUE}Operação cancelada. Nenhum serviço foi parado.${NC}"
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# Parar todos os ambientes
# ---------------------------------------------------------------------------
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  🛑 Parando todos os serviços do ambiente              ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Definir flag de volumes
VOLUME_FLAG=""
if [ "$REMOVER_VOLUMES" = true ]; then
    VOLUME_FLAG="--volumes"
    echo -e "${YELLOW}🗑️  Volumes serão removidos após parar os serviços.${NC}"
    echo ""
fi

# Parar stack completa (docker-compose.full.yml)
echo "Parando stack completa (full)..."
docker compose -f shared/docker-compose.full.yml down $VOLUME_FLAG 2>/dev/null || true

# Parar ambiente com Airflow (base + override)
echo "Parando ambiente Airflow (base + airflow override)..."
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml down $VOLUME_FLAG 2>/dev/null || true

# Parar ambiente base (Spark + Jupyter)
echo "Parando ambiente base (Spark + Jupyter)..."
docker compose -f shared/docker-compose.yml down $VOLUME_FLAG 2>/dev/null || true

# ---------------------------------------------------------------------------
# Mensagem final
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}✅ Todos os serviços foram parados com sucesso!${NC}"
if [ "$REMOVER_VOLUMES" = true ]; then
    echo -e "${YELLOW}🗑️  Volumes Docker foram removidos.${NC}"
fi
echo ""
echo "Para reiniciar o ambiente, use: ./shared/start_env.sh [base|airflow|full]"
echo ""
