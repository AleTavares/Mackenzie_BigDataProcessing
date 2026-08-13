# Projeto Final — Big Data Processing

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)  
**Professor:** Alexandre Tavares  
**Entrega:** Via formulário disponibilizado no dia da apresentação  

---

## Formato de Entrega

1. **Repositório Git** (GitHub ou GitLab) contendo todos os artefatos do projeto
2. **Apresentação em grupo** durante a Aula 8 (demo ao vivo obrigatória)
3. **Submissão via Form** — o link será disponibilizado pelo professor no dia da apresentação

O formulário solicitará:
- Nomes completos de todos os integrantes
- Link do repositório (público ou com acesso concedido ao professor)
- Descrição do projeto
- Opção escolhida (A ou B)
- Domínio/vertical (se opção B)

---

## Escolha seu Projeto

Cada grupo deve escolher **uma** das duas opções abaixo:

---

## Opção A — Pipeline de E-commerce (DataFlow Analytics)

### Cenário

A DataFlow Analytics precisa entregar um pipeline de produção para seu cliente ShopBrasil. O pipeline deve processar vendas diárias, aplicar regras de qualidade e gerar métricas de negócio para o dashboard executivo.

### Dados

Usar os datasets do curso (`datasets/`) ou gerar dados sintéticos equivalentes:
- Vendas (CSV ou Parquet) — mínimo 50K registros
- Clientes (Parquet)
- Categorias/Produtos (JSON)

### Requisitos Específicos

| Componente | Requisito |
|-----------|-----------|
| **Ingestão** | Ler pelo menos 2 fontes em formatos diferentes (CSV + JSON ou Parquet) |
| **Bronze** | Dados brutos com metadados de ingestão (`_source`, `_ingestion_ts`) |
| **Silver** | Normalização de schema, tratamento de nulls, deduplicação |
| **Gold** | Mínimo 2 tabelas agregadas: faturamento por estado + análise temporal (mensal) |
| **Qualidade** | 3 checks: completude, unicidade, validade de domínio. Quarentena funcional |
| **Orquestração** | DAG Airflow com: sensor → spark job → quality checks → notificação |
| **Docker** | `docker compose up` sobe todo o ambiente sem intervenção |

---

## Opção B — Pipeline de Domínio Livre

### Cenário

Escolha um domínio de negócio diferente de e-commerce e construa um pipeline equivalente. Exemplos:

| Domínio | Exemplo de Dados |
|---------|-----------------|
| 🏥 Saúde | Internações, procedimentos, indicadores hospitalares |
| 💰 Finanças | Transações bancárias, detecção de anomalias |
| 🚚 Logística | Entregas, rotas, SLAs de entrega |
| 📚 Educação | Matrículas, notas, evasão |
| 🌾 Agro | Produção agrícola, dados climáticos, preços |
| 🏙️ Governo | Dados abertos (IBGE, Portal da Transparência) |

### Dados

- Pode usar datasets reais públicos (Kaggle, dados.gov.br, IBGE, etc.)
- Ou gerar dados sintéticos realistas
- Mínimo 50K registros na fonte principal

### Requisitos Específicos

Mesmos requisitos técnicos da Opção A, adaptados ao domínio escolhido:

| Componente | Requisito |
|-----------|-----------|
| **Ingestão** | Pelo menos 2 fontes de dados |
| **Bronze** | Dados brutos com metadados de rastreabilidade |
| **Silver** | Limpeza, normalização e validações de negócio do domínio |
| **Gold** | Mínimo 2 tabelas agregadas relevantes para o negócio |
| **Qualidade** | 3 checks adaptados ao domínio. Quarentena funcional |
| **Orquestração** | DAG Airflow com mínimo 4 tasks encadeadas |
| **Docker** | Ambiente reproduzível com `docker compose up` |

---

## Requisitos Técnicos Obrigatórios (ambas opções)

### Stack

| Tecnologia | Versão | Obrigatório |
|-----------|--------|-------------|
| Python | 3.10+ | ✅ |
| Apache Spark (PySpark) | 3.5.x | ✅ |
| Apache Airflow | 2.8.x | ✅ |
| Docker Compose | 2.x | ✅ |
| Formato de saída | Parquet | ✅ |

### Estrutura do Repositório

```
projeto-final/
├── README.md                    # Como rodar + arquitetura + integrantes
├── docker-compose.yml           # Um comando sobe tudo
├── dags/
│   └── pipeline.py              # DAG do Airflow
├── spark_jobs/
│   ├── ingestao.py              # Bronze
│   ├── transformacao.py         # Silver
│   └── agregacao.py             # Gold
├── quality/
│   └── checks.py                # Validações de qualidade
├── data/
│   └── raw/                     # Dados de entrada (ou instruções de download)
└── docs/
    └── arquitetura.md           # Diagrama do pipeline
```

### Restrições

- Tudo roda **100% local** (sem cloud services pagos)
- Código de produção em **scripts .py** (não notebooks)
- Ambiente deve funcionar com **mínimo 8GB RAM e 4 cores**
- **Proibido** usar frameworks de qualidade prontos (Great Expectations, Soda) — implementar validações customizadas

---

## Apresentação (Aula 8)

### Formato

| Item | Duração |
|------|---------|
| Apresentação + Demo ao vivo | 20 minutos |
| Perguntas (professor + colegas) | 5 minutos |
| **Total por grupo** | **25 minutos** |

### Regras

1. **Todos os integrantes** devem participar (falar pelo menos 2 minutos cada)
2. **Demo ao vivo obrigatória** — `docker compose up` → pipeline funcionando
3. Tenha um **plano B** (screenshots/vídeo) caso Docker falhe no dia
4. Cronômetro visível — aos 20 min a apresentação é encerrada

### Estrutura Sugerida

| Tempo | Conteúdo |
|-------|----------|
| 0-3 min | Problema de negócio e dados utilizados |
| 3-6 min | Arquitetura técnica (diagrama) |
| 6-15 min | **Demo ao vivo** — pipeline rodando end-to-end |
| 15-18 min | Quality checks e resultados na Gold |
| 18-20 min | Dificuldades, aprendizados e próximos passos |
| 20-25 min | Q&A |

---

## Avaliação

A nota final do projeto é composta por:

```
Nota Final = Nota Técnica (repo) + Nota de Apresentação (professor)
```

### Nota Técnica (via repositório)

| Critério | Peso | O que é avaliado |
|----------|------|-----------------|
| Pipeline funciona | 30% | `docker compose up` → dados fluem Bronze → Silver → Gold |
| Arquitetura Medallion | 20% | Separação clara de camadas, schema correto em cada uma |
| Qualidade de dados | 20% | 3+ checks implementados, quarentena funcional, conservação |
| Orquestração (Airflow) | 15% | DAG funcional, 4+ tasks, retries, sensor ou callback |
| Documentação | 15% | README claro, diagrama de arquitetura, instruções de execução |

### Nota de Apresentação (pelo professor)

| Critério | O que é avaliado |
|----------|-----------------|
| Clareza | Explicação objetiva do problema e da solução |
| Demo | Pipeline funcionou ao vivo |
| Participação | Todos os membros falaram e demonstraram domínio |
| Tempo | Respeitou os 20 minutos |

---

## Entrega

### Prazos

| Item | Prazo |
|------|-------|
| Formação dos grupos (3-5 pessoas) | Até o final da Aula 5 |
| Repositório pronto | 48h antes da Aula 8 |
| Submissão via Form | Durante a Aula 8 (link no dia) |
| Apresentação | Durante a Aula 8 |

### O Form vai pedir:

- Nomes completos de todos os integrantes
- Link do repositório (público ou com acesso concedido ao professor)
- Descrição do projeto
- Opção escolhida (A ou B)
- Domínio/vertical (se opção B)

---

## Checklist Rápido

Antes de entregar, verifique:

- [ ] `docker compose up` sobe sem erros
- [ ] DAG aparece no Airflow UI e executa com sucesso
- [ ] Dados fluem: fonte → Bronze → Silver → Gold
- [ ] 3+ checks de qualidade executam e quarentena funciona
- [ ] README.md explica como rodar o projeto
- [ ] Diagrama de arquitetura presente
- [ ] Todos os integrantes ensaiaram a apresentação
- [ ] Demo testada pelo menos 2x em máquina limpa
- [ ] Repositório organizado conforme estrutura sugerida

---

## Dicas

- **Comece pelo Docker** — garanta que o ambiente sobe antes de escrever código
- **Use os labs como base** — os notebooks das aulas 1-7 cobrem todos os requisitos
- **Trabalhem incrementalmente** — Bronze funciona? Só então vá para Silver
- **Testem em máquina limpa** — peça a um colega para clonar e rodar
- **Ensaiem a demo** — Murphy adora apresentações ao vivo
- **Dividam responsabilidades** mas todos devem entender o pipeline inteiro

---

*"Dados sem qualidade são apenas ruído. Pipeline sem orquestração é trabalho manual."*
