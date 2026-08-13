#!/usr/bin/env bash
# =============================================================================
# Teste de Cold Start — Validação dos Labs em Docker Limpo
# Curso: Big Data Processing - MBA Engenharia de Dados (Mackenzie)
# =============================================================================
# Este script realiza um teste completo de cold start dos ambientes Docker
# utilizados no curso. Ele remove todas as imagens/containers/volumes,
# sobe cada stack sequencialmente e verifica se os serviços estão acessíveis.
#
# Uso:
#   chmod +x shared/test_cold_start.sh
#   ./shared/test_cold_start.sh              # Teste completo com limpeza
#   ./shared/test_cold_start.sh --skip-clean # Pula a limpeza (warm start)
#   ./shared/test_cold_start.sh --help       # Mostra ajuda
#
# Requisitos: Docker 24+, Docker Compose v2, curl, bash 4+
# Duração estimada: 30-45 min (cold start) | 10-15 min (warm start)
# =============================================================================

set -euo pipefail

# --- Configuração -----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SHARED_DIR="$SCRIPT_DIR"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Contadores de resultados
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# Timeout para esperar serviços (segundos)
HEALTH_TIMEOUT=180
PULL_TIMEOUT=600

# Flag de limpeza
SKIP_CLEAN=false

# --- Funções Utilitárias ----------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${YELLOW}--- $1 ---${NC}"
}

print_pass() {
    echo -e "  ${GREEN}✅ PASS${NC}: $1"
    ((PASS_COUNT++))
}

print_fail() {
    echo -e "  ${RED}❌ FAIL${NC}: $1"
    ((FAIL_COUNT++))
}

print_warn() {
    echo -e "  ${YELLOW}⚠️  WARN${NC}: $1"
    ((WARN_COUNT++))
}

print_info() {
    echo -e "  ${BLUE}ℹ️  INFO${NC}: $1"
}

# Espera um serviço HTTP ficar disponível
wait_for_http() {
    local url="$1"
    local service_name="$2"
    local timeout="${3:-$HEALTH_TIMEOUT}"
    local elapsed=0
    local interval=5

    print_info "Aguardando $service_name em $url (timeout: ${timeout}s)..."

    while [ $elapsed -lt $timeout ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
    done

    return 1
}

# Verifica se uma porta está respondendo
check_port() {
    local port="$1"
    curl -sf "http://localhost:$port" > /dev/null 2>&1
}

# Tempo formatado
format_duration() {
    local seconds=$1
    local minutes=$((seconds / 60))
    local remaining_seconds=$((seconds % 60))
    echo "${minutes}m ${remaining_seconds}s"
}

# --- Verificação de Pré-requisitos -------------------------------------------

check_prerequisites() {
    print_header "VERIFICAÇÃO DE PRÉ-REQUISITOS"

    # Docker
    if docker --version > /dev/null 2>&1; then
        local docker_version
        docker_version=$(docker --version | grep -oP '\d+\.\d+' | head -1)
        print_pass "Docker instalado (v$docker_version)"
    else
        print_fail "Docker não encontrado"
        echo "       Instale Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi

    # Docker Compose v2
    if docker compose version > /dev/null 2>&1; then
        local compose_version
        compose_version=$(docker compose version | grep -oP '\d+\.\d+' | head -1)
        print_pass "Docker Compose v2 instalado (v$compose_version)"
    else
        print_fail "Docker Compose v2 não encontrado"
        echo "       Atualize Docker para versão 20.10+ (compose integrado)"
        exit 1
    fi

    # curl
    if command -v curl > /dev/null 2>&1; then
        print_pass "curl disponível"
    else
        print_fail "curl não encontrado"
        exit 1
    fi

    # Docker daemon rodando
    if docker info > /dev/null 2>&1; then
        print_pass "Docker daemon ativo"
    else
        print_fail "Docker daemon não está rodando"
        echo "       Execute: sudo systemctl start docker"
        exit 1
    fi

    # Memória disponível
    local total_mem_kb
    total_mem_kb=$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "0")
    local total_mem_gb=$((total_mem_kb / 1024 / 1024))

    if [ "$total_mem_gb" -ge 8 ]; then
        print_pass "Memória RAM: ${total_mem_gb}GB (mínimo: 8GB)"
    elif [ "$total_mem_gb" -ge 6 ]; then
        print_warn "Memória RAM: ${total_mem_gb}GB (mínimo recomendado: 8GB)"
    else
        print_fail "Memória RAM insuficiente: ${total_mem_gb}GB (mínimo: 8GB)"
    fi

    # Espaço em disco
    local available_gb
    available_gb=$(df /var/lib/docker 2>/dev/null | tail -1 | awk '{print int($4/1024/1024)}' || echo "0")

    if [ "$available_gb" -ge 15 ]; then
        print_pass "Espaço em disco: ${available_gb}GB disponíveis (mínimo: 15GB)"
    elif [ "$available_gb" -ge 10 ]; then
        print_warn "Espaço em disco: ${available_gb}GB (recomendado: 15GB)"
    else
        print_fail "Espaço em disco insuficiente: ${available_gb}GB (mínimo: 15GB)"
    fi

    # CPUs
    local cpu_count
    cpu_count=$(nproc 2>/dev/null || echo "0")
    if [ "$cpu_count" -ge 4 ]; then
        print_pass "CPUs: $cpu_count cores (mínimo: 4)"
    else
        print_warn "CPUs: $cpu_count cores (mínimo recomendado: 4)"
    fi
}

# --- Limpeza Docker ----------------------------------------------------------

clean_docker() {
    print_header "LIMPEZA DO AMBIENTE DOCKER (COLD START)"

    if [ "$SKIP_CLEAN" = true ]; then
        print_info "Limpeza pulada (--skip-clean). Usando imagens em cache."
        return 0
    fi

    print_info "Parando todos os containers..."
    docker stop $(docker ps -aq) 2>/dev/null || true

    print_info "Removendo todos os containers..."
    docker rm $(docker ps -aq) 2>/dev/null || true

    print_info "Removendo todas as imagens..."
    docker rmi $(docker images -aq) 2>/dev/null || true

    print_info "Removendo volumes..."
    docker volume prune -f 2>/dev/null || true

    print_info "Removendo redes..."
    docker network prune -f 2>/dev/null || true

    print_info "Limpeza final..."
    docker system prune -af --volumes 2>/dev/null || true

    # Verificar que está limpo
    local image_count
    image_count=$(docker images -q | wc -l)
    if [ "$image_count" -eq 0 ]; then
        print_pass "Ambiente Docker completamente limpo (0 imagens)"
    else
        print_warn "Restaram $image_count imagens (pode ser normal em alguns ambientes)"
    fi
}

# --- Teste: Stack Base (Aulas 01-03) ----------------------------------------

test_stack_base() {
    print_header "TESTE 1/3: STACK BASE — Spark + Jupyter (Aulas 01-03)"

    local start_time=$SECONDS

    print_section "Subindo serviços"
    cd "$SHARED_DIR"
    docker compose up -d 2>&1 | tail -5

    # Aguardar serviços
    print_section "Aguardando healthchecks"

    if wait_for_http "http://localhost:8080" "Spark Master" $HEALTH_TIMEOUT; then
        print_pass "Spark Master UI acessível (porta 8080)"
    else
        print_fail "Spark Master UI não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    if wait_for_http "http://localhost:8888" "Jupyter Notebook" $HEALTH_TIMEOUT; then
        print_pass "Jupyter Notebook acessível (porta 8888)"
    else
        print_fail "Jupyter Notebook não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    # Verificar Worker registrado
    print_section "Verificando integridade dos serviços"

    sleep 10  # Dar tempo para worker registrar

    if curl -sf http://localhost:8080 | grep -q "Workers"; then
        print_pass "Spark Worker registrado no Master"
    else
        print_warn "Worker pode não ter registrado ainda (verificar manualmente)"
    fi

    # Verificar containers
    local running_count
    running_count=$(docker compose ps --status running -q 2>/dev/null | wc -l)
    if [ "$running_count" -ge 3 ]; then
        print_pass "Todos os containers running ($running_count/3)"
    else
        print_fail "Containers insuficientes running ($running_count/3)"
    fi

    # Verificar volume de datasets
    if docker exec jupyter-notebook ls /home/jovyan/work/data/ > /dev/null 2>&1; then
        print_pass "Volume de datasets montado no Jupyter"
    else
        print_warn "Volume de datasets não encontrado (verificar se datasets/ existe)"
    fi

    local duration=$((SECONDS - start_time))
    print_info "Tempo da Stack Base: $(format_duration $duration)"

    # Cleanup
    print_section "Limpando Stack Base"
    docker compose down -v 2>/dev/null
    sleep 5
}

# --- Teste: Stack Airflow (Aulas 04-05) -------------------------------------

test_stack_airflow() {
    print_header "TESTE 2/3: STACK AIRFLOW — Spark + Airflow + Jupyter (Aulas 04-05)"

    local start_time=$SECONDS

    print_section "Subindo serviços (base + airflow override)"
    cd "$SHARED_DIR"
    docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d 2>&1 | tail -5

    # Aguardar serviços
    print_section "Aguardando healthchecks"

    if wait_for_http "http://localhost:8080" "Spark Master" $HEALTH_TIMEOUT; then
        print_pass "Spark Master UI acessível (porta 8080)"
    else
        print_fail "Spark Master UI não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    if wait_for_http "http://localhost:8888" "Jupyter Notebook" $HEALTH_TIMEOUT; then
        print_pass "Jupyter Notebook acessível (porta 8888)"
    else
        print_fail "Jupyter Notebook não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    # Airflow demora mais para inicializar
    if wait_for_http "http://localhost:8081/health" "Airflow Webserver" 240; then
        print_pass "Airflow Webserver saudável (porta 8081)"
    else
        print_fail "Airflow Webserver não respondeu em 240s"
    fi

    # Verificações adicionais
    print_section "Verificando integridade dos serviços"

    # Airflow Init completou?
    local init_exit
    init_exit=$(docker inspect airflow-init --format='{{.State.ExitCode}}' 2>/dev/null || echo "unknown")
    if [ "$init_exit" = "0" ]; then
        print_pass "Airflow Init completou com sucesso (exit code 0)"
    else
        print_fail "Airflow Init falhou (exit code: $init_exit)"
    fi

    # Scheduler rodando?
    if docker ps --filter "name=airflow-scheduler" --filter "status=running" -q | grep -q .; then
        print_pass "Airflow Scheduler está running"
    else
        print_fail "Airflow Scheduler não está running"
    fi

    # DAGs da Aula 04 montadas?
    if docker exec airflow-scheduler ls /opt/airflow/dags/aula_04/ > /dev/null 2>&1; then
        print_pass "DAGs da Aula 04 montadas no volume"
    else
        print_warn "DAGs da Aula 04 não encontradas (verificar pasta aula_04/code/dags/)"
    fi

    # Verificar Worker Spark
    sleep 10
    if curl -sf http://localhost:8080 | grep -q "Workers"; then
        print_pass "Spark Worker registrado no Master"
    else
        print_warn "Worker pode não ter registrado (verificar manualmente)"
    fi

    local duration=$((SECONDS - start_time))
    print_info "Tempo da Stack Airflow: $(format_duration $duration)"

    # Cleanup
    print_section "Limpando Stack Airflow"
    docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v 2>/dev/null
    sleep 5
}

# --- Teste: Stack Completa (Aulas 06-07) ------------------------------------

test_stack_full() {
    print_header "TESTE 3/3: STACK COMPLETA — Produção (Aulas 06-07)"

    local start_time=$SECONDS

    print_section "Subindo stack completa de produção"
    cd "$SHARED_DIR"
    docker compose -f docker-compose.full.yml up -d 2>&1 | tail -5

    # Aguardar serviços
    print_section "Aguardando healthchecks"

    if wait_for_http "http://localhost:8080" "Spark Master" $HEALTH_TIMEOUT; then
        print_pass "Spark Master UI acessível (porta 8080)"
    else
        print_fail "Spark Master UI não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    if wait_for_http "http://localhost:8888" "Jupyter Notebook" $HEALTH_TIMEOUT; then
        print_pass "Jupyter Notebook acessível (porta 8888)"
    else
        print_fail "Jupyter Notebook não respondeu em ${HEALTH_TIMEOUT}s"
    fi

    if wait_for_http "http://localhost:8081/health" "Airflow Webserver" 240; then
        print_pass "Airflow Webserver saudável (porta 8081)"
    else
        print_fail "Airflow Webserver não respondeu em 240s"
    fi

    # Verificações de integridade
    print_section "Verificando integridade dos serviços"

    # Airflow Init completou?
    local init_exit
    init_exit=$(docker inspect airflow-init --format='{{.State.ExitCode}}' 2>/dev/null || echo "unknown")
    if [ "$init_exit" = "0" ]; then
        print_pass "Airflow Init completou com sucesso"
    else
        print_fail "Airflow Init falhou (exit code: $init_exit)"
    fi

    # Scheduler
    if docker ps --filter "name=airflow-scheduler" --filter "status=running" -q | grep -q .; then
        print_pass "Airflow Scheduler running"
    else
        print_fail "Airflow Scheduler não running"
    fi

    # Worker
    sleep 10
    if curl -sf http://localhost:8080 | grep -q "Workers"; then
        print_pass "Spark Worker registrado"
    else
        print_warn "Worker pode não ter registrado"
    fi

    # Teste de integração: Jupyter → Spark
    print_section "Teste de integração (Jupyter → Spark cluster)"

    local spark_test_result
    spark_test_result=$(docker exec jupyter-notebook python -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('cold-start-test').master('spark://spark-master:7077').getOrCreate()
    df = spark.range(100)
    count = df.count()
    spark.stop()
    if count == 100:
        print('OK')
    else:
        print('FAIL')
except Exception as e:
    print(f'ERROR: {e}')
" 2>/dev/null || echo "ERROR")

    if echo "$spark_test_result" | grep -q "OK"; then
        print_pass "Integração Jupyter → Spark funcional (job executou no cluster)"
    else
        print_fail "Integração Jupyter → Spark falhou ($spark_test_result)"
    fi

    # Datasets
    if docker exec jupyter-notebook ls /home/jovyan/work/data/ > /dev/null 2>&1; then
        print_pass "Datasets montados e acessíveis"
    else
        print_warn "Datasets não encontrados no Jupyter"
    fi

    # Containers ativos
    local running_count
    running_count=$(docker compose -f docker-compose.full.yml ps --status running -q 2>/dev/null | wc -l)
    if [ "$running_count" -ge 5 ]; then
        print_pass "Todos os serviços running ($running_count/5)"
    else
        print_warn "Nem todos os serviços estão running ($running_count/5)"
    fi

    local duration=$((SECONDS - start_time))
    print_info "Tempo da Stack Completa: $(format_duration $duration)"

    # Cleanup
    print_section "Limpando Stack Completa"
    docker compose -f docker-compose.full.yml down -v 2>/dev/null
}

# --- Relatório Final ---------------------------------------------------------

print_report() {
    local total_duration=$((SECONDS - TOTAL_START))

    print_header "RELATÓRIO FINAL — TESTE DE COLD START"

    echo -e "  ${GREEN}PASS${NC}: $PASS_COUNT"
    echo -e "  ${RED}FAIL${NC}: $FAIL_COUNT"
    echo -e "  ${YELLOW}WARN${NC}: $WARN_COUNT"
    echo ""
    echo -e "  Tempo total: $(format_duration $total_duration)"
    echo -e "  Data: $(date '+%Y-%m-%d %H:%M:%S')"
    echo -e "  Máquina: $(hostname)"
    echo ""

    if [ $FAIL_COUNT -eq 0 ]; then
        echo -e "  ${GREEN}════════════════════════════════════════${NC}"
        echo -e "  ${GREEN}  RESULTADO: APROVADO ✅               ${NC}"
        echo -e "  ${GREEN}  Todos os labs funcionam em cold start ${NC}"
        echo -e "  ${GREEN}════════════════════════════════════════${NC}"
        exit 0
    else
        echo -e "  ${RED}════════════════════════════════════════${NC}"
        echo -e "  ${RED}  RESULTADO: REPROVADO ❌               ${NC}"
        echo -e "  ${RED}  $FAIL_COUNT verificações falharam.    ${NC}"
        echo -e "  ${RED}  Consulte: shared/teste_docker_cold_start.md${NC}"
        echo -e "  ${RED}════════════════════════════════════════${NC}"
        exit 1
    fi
}

# --- Ajuda -------------------------------------------------------------------

show_help() {
    echo "Uso: $0 [opções]"
    echo ""
    echo "Opções:"
    echo "  --skip-clean    Pula a limpeza do Docker (warm start)"
    echo "  --help          Mostra esta mensagem"
    echo ""
    echo "Descrição:"
    echo "  Testa todos os ambientes Docker do curso em cold start."
    echo "  Remove imagens, sobe cada stack e valida serviços."
    echo ""
    echo "Duração estimada:"
    echo "  Cold start (completo): 30-45 min"
    echo "  Warm start (--skip-clean): 10-15 min"
}

# --- Main --------------------------------------------------------------------

# Processar argumentos
for arg in "$@"; do
    case $arg in
        --skip-clean)
            SKIP_CLEAN=true
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Argumento desconhecido: $arg"
            show_help
            exit 1
            ;;
    esac
done

# Início
TOTAL_START=$SECONDS

print_header "TESTE DE COLD START — Big Data Processing (Mackenzie)"
echo "  Diretório do projeto: $PROJECT_ROOT"
echo "  Modo: $([ "$SKIP_CLEAN" = true ] && echo 'Warm Start' || echo 'Cold Start (limpeza total)')"
echo "  Início: $(date '+%Y-%m-%d %H:%M:%S')"

# Confirmação de segurança (apenas em cold start)
if [ "$SKIP_CLEAN" = false ]; then
    echo ""
    echo -e "  ${RED}⚠️  ATENÇÃO: Este script irá REMOVER TODAS as imagens,${NC}"
    echo -e "  ${RED}    containers e volumes Docker desta máquina!${NC}"
    echo ""
    read -p "  Deseja continuar? (s/N): " confirm
    if [[ ! "$confirm" =~ ^[sS]$ ]]; then
        echo "  Cancelado pelo usuário."
        exit 0
    fi
fi

# Executar testes
check_prerequisites
clean_docker
test_stack_base
test_stack_airflow
test_stack_full
print_report
