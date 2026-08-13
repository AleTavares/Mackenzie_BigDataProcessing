#!/bin/bash
# =============================================================================
# reset_data.sh - Resetar dados para estado original
# Curso: Big Data Processing - MBA Engenharia de Dados (Mackenzie)
# =============================================================================
# Script para recuperar o estado inicial dos dados entre laboratórios.
# Remove diretórios gerados durante os labs e opcionalmente limpa notebooks.
# Deve ser executado a partir da RAIZ do repositório.
#
# Uso:
#   ./shared/reset_data.sh              # Reset dos dados de trabalho
#   ./shared/reset_data.sh --notebooks  # Também limpa outputs dos notebooks
#   ./shared/reset_data.sh --help       # Exibir ajuda
# =============================================================================
set -e

# Cores para mensagens no terminal
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # Sem cor

# Diretório raiz do projeto (onde o script é executado)
PROJECT_ROOT="$(pwd)"

# ---------------------------------------------------------------------------
# Função: exibir mensagem de ajuda
# ---------------------------------------------------------------------------
mostrar_ajuda() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  🔄 Big Data Processing - Resetar Dados do Laboratório     ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Uso: ./shared/reset_data.sh [OPÇÕES]"
    echo ""
    echo "Opções:"
    echo "  --notebooks, -n   Também limpa outputs dos Jupyter notebooks"
    echo "                    (preserva o código, remove apenas saídas)"
    echo "  --help, -h        Exibe esta mensagem de ajuda"
    echo ""
    echo "O que é resetado:"
    echo "  • Diretório datalake/ (criado durante labs da Aula 3+)"
    echo "  • Diretórios de trabalho (work/) dentro de cada aula"
    echo "  • Arquivos temporários e outputs gerados durante os labs"
    echo ""
    echo "O que NÃO é alterado:"
    echo "  • Datasets originais em datasets/"
    echo "  • Código fonte e configurações"
    echo "  • Docker Compose files e configurações de ambiente"
    echo ""
    echo "Exemplos:"
    echo "  ./shared/reset_data.sh              # Reset básico"
    echo "  ./shared/reset_data.sh --notebooks  # Reset + limpar notebooks"
    echo ""
}

# ---------------------------------------------------------------------------
# Processar argumentos
# ---------------------------------------------------------------------------
LIMPAR_NOTEBOOKS=false

case "${1:-}" in
    --help|-h)
        mostrar_ajuda
        exit 0
        ;;
    --notebooks|-n)
        LIMPAR_NOTEBOOKS=true
        ;;
    "")
        # Sem argumentos - comportamento padrão
        ;;
    *)
        echo -e "${YELLOW}❌ Erro: Opção '$1' não reconhecida.${NC}"
        echo "Use './shared/reset_data.sh --help' para ver as opções disponíveis."
        exit 1
        ;;
esac

# ---------------------------------------------------------------------------
# Início do reset
# ---------------------------------------------------------------------------
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  🔄 Resetando dados para estado original               ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Contador de itens removidos
ITENS_REMOVIDOS=0

# ---------------------------------------------------------------------------
# 1. Remover diretório datalake/ (criado durante labs da Aula 3+)
# ---------------------------------------------------------------------------
echo -e "${GREEN}[1/4]${NC} Verificando diretório datalake/..."
if [ -d "$PROJECT_ROOT/datalake" ]; then
    rm -rf "$PROJECT_ROOT/datalake"
    echo "      ✅ Removido: datalake/"
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
else
    echo "      ⏭️  Não encontrado (já limpo)"
fi

# ---------------------------------------------------------------------------
# 2. Remover diretórios de trabalho (work/) de cada aula
# ---------------------------------------------------------------------------
echo -e "${GREEN}[2/4]${NC} Verificando diretórios de trabalho das aulas..."
for aula_dir in "$PROJECT_ROOT"/aula_*/; do
    if [ -d "$aula_dir" ]; then
        # Remover diretório work/ se existir
        if [ -d "${aula_dir}work" ]; then
            rm -rf "${aula_dir}work"
            echo "      ✅ Removido: $(basename "$aula_dir")/work/"
            ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
        fi
        # Remover diretório output/ se existir
        if [ -d "${aula_dir}output" ]; then
            rm -rf "${aula_dir}output"
            echo "      ✅ Removido: $(basename "$aula_dir")/output/"
            ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
        fi
        # Remover diretório temp/ se existir
        if [ -d "${aula_dir}temp" ]; then
            rm -rf "${aula_dir}temp"
            echo "      ✅ Removido: $(basename "$aula_dir")/temp/"
            ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
        fi
    fi
done
if [ $ITENS_REMOVIDOS -eq 0 ]; then
    echo "      ⏭️  Nenhum diretório de trabalho encontrado"
fi

# ---------------------------------------------------------------------------
# 3. Remover arquivos temporários gerados durante labs
# ---------------------------------------------------------------------------
echo -e "${GREEN}[3/4]${NC} Removendo arquivos temporários..."
TEMP_ANTES=$ITENS_REMOVIDOS

# Remover checkpoints do Spark (.crc files e _SUCCESS markers gerados)
find "$PROJECT_ROOT" -name "*.crc" -not -path "*/datasets/*" -delete 2>/dev/null && \
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1)) || true
find "$PROJECT_ROOT" -name "_SUCCESS" -not -path "*/datasets/*" -delete 2>/dev/null && \
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1)) || true

# Remover diretórios __pycache__
find "$PROJECT_ROOT" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Remover metastore_db do Spark (criado durante labs)
if [ -d "$PROJECT_ROOT/metastore_db" ]; then
    rm -rf "$PROJECT_ROOT/metastore_db"
    echo "      ✅ Removido: metastore_db/"
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
fi

# Remover derby.log (criado pelo Spark)
if [ -f "$PROJECT_ROOT/derby.log" ]; then
    rm -f "$PROJECT_ROOT/derby.log"
    echo "      ✅ Removido: derby.log"
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
fi

# Remover spark-warehouse (criado durante labs)
if [ -d "$PROJECT_ROOT/spark-warehouse" ]; then
    rm -rf "$PROJECT_ROOT/spark-warehouse"
    echo "      ✅ Removido: spark-warehouse/"
    ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + 1))
fi

if [ $ITENS_REMOVIDOS -eq $TEMP_ANTES ]; then
    echo "      ⏭️  Nenhum arquivo temporário encontrado"
fi

# ---------------------------------------------------------------------------
# 4. Limpar outputs dos Jupyter notebooks (opcional)
# ---------------------------------------------------------------------------
echo -e "${GREEN}[4/4]${NC} Verificando Jupyter notebooks..."
if [ "$LIMPAR_NOTEBOOKS" = true ]; then
    # Verificar se jupyter nbconvert está disponível
    if command -v jupyter &> /dev/null; then
        NOTEBOOKS_ENCONTRADOS=0
        while IFS= read -r -d '' notebook; do
            jupyter nbconvert --clear-output --inplace "$notebook" 2>/dev/null && \
                NOTEBOOKS_ENCONTRADOS=$((NOTEBOOKS_ENCONTRADOS + 1)) || true
        done < <(find "$PROJECT_ROOT" -name "*.ipynb" -not -path "*/.ipynb_checkpoints/*" -print0)

        if [ $NOTEBOOKS_ENCONTRADOS -gt 0 ]; then
            echo "      ✅ Outputs limpos em $NOTEBOOKS_ENCONTRADOS notebook(s)"
            ITENS_REMOVIDOS=$((ITENS_REMOVIDOS + NOTEBOOKS_ENCONTRADOS))
        else
            echo "      ⏭️  Nenhum notebook encontrado"
        fi
    else
        # Fallback: limpar via Python se jupyter CLI não estiver disponível
        if command -v python3 &> /dev/null; then
            python3 -c "
import json, glob, sys

notebooks = glob.glob('$PROJECT_ROOT/**/*.ipynb', recursive=True)
notebooks = [n for n in notebooks if '.ipynb_checkpoints' not in n]
count = 0
for nb_path in notebooks:
    try:
        with open(nb_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        for cell in nb.get('cells', []):
            if cell.get('cell_type') == 'code':
                cell['outputs'] = []
                cell['execution_count'] = None
        with open(nb_path, 'w', encoding='utf-8') as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
            f.write('\n')
        count += 1
    except Exception:
        pass
print(f'      ✅ Outputs limpos em {count} notebook(s)' if count > 0 else '      ⏭️  Nenhum notebook encontrado')
" 2>/dev/null || echo "      ⚠️  Não foi possível limpar notebooks (jupyter/python3 não disponível)"
        else
            echo "      ⚠️  Não foi possível limpar notebooks (jupyter/python3 não disponível)"
        fi
    fi
else
    echo "      ⏭️  Pular (use --notebooks para limpar outputs)"
fi

# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Reset concluído!${NC}"
echo ""
if [ $ITENS_REMOVIDOS -gt 0 ]; then
    echo "   $ITENS_REMOVIDOS item(ns) removido(s)/limpo(s)."
else
    echo "   Nenhuma alteração necessária - os dados já estavam no estado original."
fi
echo ""
echo "Os datasets originais em datasets/ permanecem intactos."
echo "Você pode iniciar os labs novamente com: ./shared/start_env.sh"
echo ""
