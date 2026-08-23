#!/bin/bash

# Pergunta de segurança
read -p "⚠️  Isso vai apagar TODOS os dados do Docker. Continuar? (s/n): " confirmacao

if [[ "$confirmacao" != "s" && "$confirmacao" != "S" ]]; then
    echo "❌ Operação cancelada pelo usuário."
    exit 1
fi

echo "🛑 Parando todos os containers ativos..."
docker stop $(docker ps -q) 2>/dev/null

echo "⏹️ Parando e removendo serviços do Docker Compose..."
docker compose down --volumes --remove-orphans 2>/dev/null

echo "🧹 Iniciando a limpeza profunda do Docker..."
docker system prune -a --volumes -f

echo "✅ Limpeza concluída com sucesso!"
