# DataFlow Analytics — Background da Empresa

## Visão Geral

A **DataFlow Analytics** é uma startup brasileira especializada em inteligência de dados para o setor de e-commerce e varejo. A empresa desenvolve uma plataforma de analytics que processa dados de vendas de múltiplos e-commerces, gerando insights acionáveis e relatórios automatizados para seus clientes.

Fundada em 2021 em São Paulo, a DataFlow nasceu da percepção de que o mercado brasileiro de e-commerce — avaliado em R$ 185 bilhões — carecia de soluções acessíveis de análise de dados para pequenas e médias operações de varejo digital. Enquanto grandes players como Magazine Luiza e Mercado Livre possuem equipes internas robustas de dados, milhares de e-commerces menores operam às cegas, sem visibilidade sobre padrões de compra, sazonalidade ou eficiência logística.

---

## Fundação

A DataFlow Analytics foi fundada em março de 2021 por dois profissionais complementares:

- **Marina Silva (CTO)** — Engenheira de software com 12 anos de experiência em empresas de tecnologia (passagens por TOTVS, iFood e Nubank). Especialista em arquitetura de dados e sistemas distribuídos. Marina é a mente técnica por trás da plataforma e lidera todas as decisões de engenharia.

- **Roberto Tanaka (CEO)** — Administrador com MBA em Gestão de Tecnologia, com 15 anos de experiência no varejo digital. Trabalhou na B2W (atual Americanas S.A.) e em consultorias de gestão. Roberto entende profundamente as dores do e-commerce brasileiro e é responsável pela estratégia comercial e captação de investimentos.

Os dois se conheceram em um meetup de dados em São Paulo em 2020, durante a pandemia. A conversa sobre as oportunidades desperdiçadas no varejo digital brasileiro evoluiu para um plano de negócios e, seis meses depois, para a constituição formal da empresa.

---

## Histórico e Evolução

### 2021 — Fundação e Primeiros Clientes

| Aspecto | Detalhe |
|---------|---------|
| Equipe | 5 pessoas (2 fundadores + 3 engenheiros) |
| Clientes | 3 e-commerces de moda em SP |
| Receita | ~R$ 180K ARR |
| Stack técnica | Python, pandas, PostgreSQL, scripts cron |
| Escritório | Coworking na Vila Madalena |

A primeira versão da plataforma era, na prática, um conjunto de scripts Python que rodavam em cron jobs num servidor EC2. Marina escreveu pessoalmente os primeiros pipelines em pandas para processar CSVs enviados por e-mail pelos clientes. O "produto" era um dashboard no Metabase atualizado diariamente.

Os desafios eram simples: garantir que os scripts rodassem todo dia, lidar com CSVs mal-formatados e entregar relatórios semanais por PDF. Funcionava — mas não escalava.

### 2022 — Crescimento 10x e Primeira Rodada de Investimento

| Aspecto | Detalhe |
|---------|---------|
| Equipe | 20 funcionários (8 engenharia, 5 produto, 4 comercial, 3 operações) |
| Clientes | 15 e-commerces em SP e RJ |
| Receita | ~R$ 2.1M ARR |
| Stack técnica | Python, pandas, PostgreSQL, Airflow (básico), AWS S3 |
| Escritório | Sala própria na Vila Olímpia |
| Investimento | Seed de R$ 4M (fundo Canary + anjos) |

O boca-a-boca trouxe uma enxurrada de clientes. O volume de dados saltou de gigabytes para dezenas de gigabytes por mês. Os scripts em pandas começaram a falhar com erros de memória. Marina contratou Carlos Mendes como engenheiro sênior para começar a profissionalizar a infraestrutura.

Nesse ano, a empresa experimentou suas primeiras dores de crescimento:
- Scripts que levavam 2 horas para rodar e falhavam no meio
- Dados inconsistentes entre relatórios (falta de idempotência)
- Engenheiros rodando pipelines manualmente às 6h da manhã
- Clientes reclamando de atrasos nos relatórios

A rodada seed permitiu contratar mais engenheiros e alugar infraestrutura na AWS, mas os problemas arquiteturais persistiam. A dívida técnica acumulada em 2021 cobrava seu preço.

### 2023 — Expansão Nacional e Crise de Escala

| Aspecto | Detalhe |
|---------|---------|
| Equipe | 45 funcionários (15 engenharia, 10 produto, 12 comercial, 8 operações) |
| Clientes | 50+ e-commerces em todo o Brasil |
| Receita | ~R$ 8.5M ARR |
| Stack técnica | Python, pandas (problemático), PostgreSQL, Airflow (parcial), AWS |
| Sede | Escritório de 300m² na Vila Olímpia |
| Desafio | Migração para infraestrutura profissional de dados |

A expansão para fora do eixo SP-RJ trouxe novos parceiros de dados em formatos completamente diferentes. Alguns enviam CSV com encoding ISO-8859-1 e separador ponto-e-vírgula. Outros disponibilizam APIs REST com payloads JSON. Os mais maduros enviam Parquet via S3.

O time de dados está no limite. Os problemas são sistêmicos:
- **Performance**: Processamentos que deveriam levar minutos levam horas em pandas
- **Confiabilidade**: Pipelines falham silenciosamente e ninguém percebe até o cliente reclamar
- **Qualidade**: Dados duplicados, campos nulos inesperados, valores negativos em quantidades
- **Orquestração**: Dependências entre jobs são gerenciadas por "ordem mental" dos engenheiros
- **Observabilidade**: Sem logs estruturados, sem métricas, sem alertas automáticos

Marina convoca uma reunião de emergência com o time de dados. A decisão é clara: **migrar de soluções artesanais (pandas scripts em cron) para infraestrutura profissional de dados** — Apache Spark para processamento distribuído, Apache Airflow para orquestração e Docker para padronização de ambientes.

Este é o arco narrativo central do curso.

---

## Mercado de Atuação

### O E-commerce Brasileiro

O mercado de e-commerce no Brasil movimentou **R$ 185 bilhões em 2023** (fonte ficcional alinhada com dados reais da ABComm). Existem aproximadamente 1,9 milhão de lojas virtuais ativas no país, das quais menos de 5% possuem capacidade interna de análise de dados sofisticada.

### Oportunidade da DataFlow

A DataFlow se posiciona no segmento de **e-commerces de médio porte** — empresas com faturamento entre R$ 5M e R$ 200M anuais que:

- Geram dados de vendas em volume significativo (100K+ transações/mês)
- Não possuem equipe interna de engenharia de dados
- Precisam de insights para competir com players maiores
- Buscam solução acessível (SaaS) em vez de construir internamente

### Proposta de Valor

> "Transformamos dados brutos de vendas em inteligência de negócio — para que e-commerces de médio porte tomem decisões com a mesma sofisticação dos grandes players."

### Concorrentes (ficcionais)

| Concorrente | Posicionamento | Limitação |
|-------------|----------------|-----------|
| DataStar | Enterprise (R$ 500K+/ano) | Caro demais para médio porte |
| QuickBI | Self-service BI | Sem processamento pesado |
| InfoVarejo | Relatórios estáticos | Sem análise preditiva |

---

## Produto: Plataforma DataFlow

### Funcionalidades Principais

1. **Ingestão Multi-formato**: Conecta-se a diversas fontes de dados (CSV, JSON, Parquet, APIs REST, bancos de dados)
2. **Processamento e Transformação**: Pipeline de ETL que normaliza, limpa e enriquece os dados
3. **Data Lake Interno**: Armazenamento estruturado em camadas (Bronze/Silver/Gold)
4. **Analytics e Relatórios**: Dashboards automatizados com KPIs de vendas, logística e marketing
5. **Alertas Inteligentes**: Notificações sobre anomalias (queda de vendas, picos inesperados, problemas de estoque)

### Arquitetura Atual (problemática)

```
[Clientes] → [CSV/Email/API] → [Scripts Python/pandas] → [PostgreSQL] → [Metabase]
                                        ↑
                                   (cron jobs)
                                   (falha silenciosa)
                                   (sem monitoramento)
```

### Arquitetura Alvo (pós-migração)

```
[Clientes] → [Ingestão Multi-formato] → [Apache Spark] → [Data Lake] → [Dashboards]
                                               ↑                ↑
                                         [Apache Airflow]  [Quality Checks]
                                         (orquestração)    (validação)
                                               ↑
                                         [Docker/Infra]
                                         (reproduzível)
```

---

## Valores da Empresa

A cultura da DataFlow é construída sobre quatro pilares:

### 🚀 Inovação
> "Não resolvemos problemas de amanhã com ferramentas de ontem."

A DataFlow incentiva experimentação técnica e busca constante por soluções melhores. A decisão de migrar para Spark e Airflow — mesmo com o custo de curto prazo — reflete esse valor.

### 🔍 Transparência
> "Dados não mentem — e nós também não."

Tanto internamente (comunicação aberta entre equipes) quanto externamente (honestidade com clientes sobre limitações e prazos). Quando um relatório está atrasado, o cliente sabe antes de perguntar.

### ⚙️ Excelência Técnica
> "Código é como produto: precisa ser confiável, testado e bem documentado."

A empresa valoriza engenharia de qualidade: code reviews, documentação, testes automatizados e boas práticas. A dívida técnica de 2021 serviu como lição sobre o custo de atalhos.

### 🤝 Colaboração
> "Nenhum pipeline roda sozinho — e nenhum engenheiro deve trabalhar isolado."

O time funciona em squads multidisciplinares. Engenheiros de dados trabalham lado a lado com analistas de produto e time comercial para garantir que a tecnologia serve ao negócio.

---

## Desafio Atual (2023) — O Arco Narrativo do Curso

### Contexto da Crise

Em outubro de 2023, três eventos simultâneos forçam a decisão de migração:

1. **Black Friday se aproximando**: O maior pico de vendas do ano vai gerar 10x mais dados que o normal. A infraestrutura atual não suporta.

2. **Cliente premium ameaça sair**: A MegaShop (maior cliente, R$ 800K/ano) recebeu relatório com dados duplicados e deu ultimato de 60 dias para resolver.

3. **Investidores pressionam**: A próxima rodada (Series A) exige demonstração de escalabilidade técnica. "Vocês não podem levantar R$ 20M rodando pandas em cron", disse o lead investor.

### Decisão Técnica

Marina apresenta ao board um plano de migração em 7 fases:

| Fase | Foco | Tecnologia | Aula |
|------|------|------------|------|
| 1 | Processamento distribuído | Apache Spark | Aula 1 |
| 2 | Transformações complexas | Spark avançado | Aula 2 |
| 3 | Ingestão multi-formato | Spark + Data Lake | Aula 3 |
| 4 | Automação básica | Apache Airflow | Aula 4 |
| 5 | Orquestração avançada | Airflow + Spark | Aula 5 |
| 6 | Qualidade e monitoramento | Framework DQ | Aula 6 |
| 7 | Pipeline de produção | Stack completa | Aula 7 |

### O Que Está em Jogo

- Se a migração der certo: Series A de R$ 20M, expansão para LATAM, 200 clientes até 2025
- Se falhar: perda do cliente premium, investidores desistem, empresa estagna

---

## Sede

**Localização**: Rua Funchal, 538 — Vila Olímpia, São Paulo - SP

A Vila Olímpia é o principal hub de startups de tecnologia de São Paulo, abrigando escritórios de empresas como iFood, 99, Loft e dezenas de scale-ups. A DataFlow ocupa um andar de 300m² em um edifício moderno com espaço aberto, salas de reunião com vidro e uma área de descompressão.

**Horário de funcionamento**: Regime híbrido — 3 dias presenciais, 2 dias remotos. O time de dados costuma trabalhar presencialmente às terças, quartas e quintas.

**Infraestrutura local**:
- 2 salas de squad (6-8 pessoas cada)
- 1 war room para incidentes
- 1 sala de reunião com monitor 65" para demos
- Cozinha com café, frutas e snacks (cultura de startup)
- Internet redundante 1Gbps (essencial para o time de dados)

---

## Equipe de Dados

O time de engenharia de dados da DataFlow é composto por **8 profissionais** com perfis complementares:

### Liderança Técnica

| Personagem | Cargo | Experiência | Papel no Curso |
|------------|-------|-------------|----------------|
| **Marina Silva** | CTO / Tech Lead de Dados | 12 anos | Mentora técnica — apresenta os desafios e guia as decisões arquiteturais |
| **Carlos Mendes** | Engenheiro de Dados Sênior | 8 anos | Guia dos labs — demonstra implementações e resolve problemas práticos |

### Negócio e Produto

| Personagem | Cargo | Experiência | Papel no Curso |
|------------|-------|-------------|----------------|
| **Ana Rodrigues** | Product Owner | 6 anos | Traz requisitos de negócio — representa a voz do cliente e define prioridades |
| **Roberto Tanaka** | CEO | 15 anos | Cobra resultados — representa pressão executiva e visão estratégica |

### Time de Engenharia de Dados (backgrounds)

| Nome | Cargo | Especialidade |
|------|-------|---------------|
| Lucas Ferreira | Engenheiro de Dados Pleno | Pipelines Python, SQL avançado |
| Juliana Costa | Engenheira de Dados Pleno | Spark, processamento distribuído |
| Pedro Santos | Engenheiro de Dados Júnior | Python, primeiros passos com Airflow |
| Rafaela Lima | Engenheira de Dados Júnior | Ingestão de dados, qualidade |
| Thiago Oliveira | DevOps / DataOps | Docker, CI/CD, infraestrutura |
| Beatriz Nakamura | Analista de Dados | SQL, visualização, Metabase |

### Dinâmica do Time

O time de dados opera em **dois squads**:

- **Squad Ingestão** (Lucas, Rafaela, Pedro): Responsável por receber e normalizar dados dos parceiros
- **Squad Analytics** (Juliana, Beatriz, Thiago): Responsável por processamento, qualidade e entrega de relatórios

Carlos coordena os dois squads tecnicamente, enquanto Ana prioriza o backlog de demandas. Marina intervém em decisões arquiteturais estratégicas.

---

## Resumo para Referência Rápida

| Item | Valor |
|------|-------|
| Nome | DataFlow Analytics |
| Tipo | Startup SaaS B2B |
| Setor | Inteligência de dados para e-commerce |
| Fundação | Março de 2021 |
| Sede | Vila Olímpia, São Paulo - SP |
| Fundadores | Marina Silva (CTO) e Roberto Tanaka (CEO) |
| Equipe total | 45 funcionários |
| Equipe de dados | 8 engenheiros |
| Clientes | 50+ e-commerces |
| Receita | ~R$ 8.5M ARR |
| Mercado alvo | E-commerce brasileiro (R$ 185 bi) |
| Stack atual | Python/pandas, PostgreSQL, cron |
| Stack alvo | Spark, Airflow, Docker, Data Lake |
| Desafio central | Migrar de soluções artesanais para infra profissional |
| Motivação | Black Friday + cliente ameaçando sair + pressão de investidores |

---

*Este documento é a base narrativa para todas as 8 aulas do curso Big Data Processing. Cada aula representa uma fase da migração tecnológica da DataFlow Analytics, contextualizada em desafios reais de negócio que a empresa enfrenta durante seu crescimento acelerado.*
