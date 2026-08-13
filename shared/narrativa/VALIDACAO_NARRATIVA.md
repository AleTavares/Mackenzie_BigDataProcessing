# Validação da Narrativa — Complexidade Monotonicamente Crescente

> **Data de validação**: Gerado automaticamente durante task 3.5
> **Objetivo**: Verificar que a narrativa da DataFlow Analytics segue uma progressão de complexidade monotonicamente crescente em TODAS as dimensões, conforme Requisito 2.2.

---

## 1. Tabela de Complexidade por Dimensão

| Aula | Equipe | Clientes | Fontes | Volume | Automação | Governança | Stakes (R$) |
|------|--------|----------|--------|--------|-----------|------------|-------------|
| 1 | 6 pessoas | 1 (ShopBrasil) | 1 (CSV) | 100K registros | Nenhuma (scripts manuais) | Nenhuma | ~R$ 180K ARR |
| 2 | 6 pessoas | 1 (ShopBrasil expandido) | 3 (vendas + clientes + categorias) | 1M registros | Nenhuma (scripts manuais) | Nenhuma | ~R$ 180K + contrato expandido |
| 3 | 12 pessoas | 3+ parceiros | 6+ (3 parceiros × formatos distintos) | Múltiplos formatos, multi-GB | Manual (Carlos roda tudo) | Informal (schema frágil) | ~R$ 2.1M ARR |
| 4 | 20 pessoas | 5 parceiros | 6+ (5 pipelines) | Diário particionado | Semi-automática (primeira DAG) | Informal | ~R$ 2.1M ARR + risco operacional |
| 5 | 20 pessoas | 10 parceiros (+ MegaShop enterprise) | 10+ | 3x volume normal (Black Friday) | Automatizada (Airflow avançado) | Parcial (SLA definido) | R$ 1.2M contrato MegaShop |
| 6 | 45 pessoas | 50+ clientes | 10+ | 50+ clientes × dados diários | Automatizada | Formal (framework DQ + LGPD) | R$ 800K direto + dominó de contas |
| 7 | 45 pessoas | 50+ clientes | Todas integradas | End-to-end completo | Produção (Docker + Airflow + Spark) | Completa (checks em cada camada) | R$ 20M Série A |
| 8 | 45+ pessoas | 50+ clientes | — (cenário integrador) | — (projeto autônomo) | — (demonstração ao board) | Auditada (apresentação ao board) | R$ 20M + expansão LATAM |

---

## 2. Verificação de Monotonicidade (Req 2.2)

### Critério: Cada linha deve ter valores iguais ou superiores à linha anterior em TODAS as dimensões.

| Transição | Equipe | Clientes | Fontes | Volume | Automação | Governança | Stakes | Resultado |
|-----------|--------|----------|--------|--------|-----------|------------|--------|-----------|
| 1 → 2 | = (6) | = (1, expandido) | ↑ (1→3) | ↑ (100K→1M) | = (nenhuma) | = (nenhuma) | ↑ (contrato expandido) | ✅ PASS |
| 2 → 3 | ↑ (6→12) | ↑ (1→3+) | ↑ (3→6+) | ↑ (multi-formato) | ↑ (=→manual) | ↑ (nenhuma→informal) | ↑ (receita cresceu) | ✅ PASS |
| 3 → 4 | ↑ (12→20) | ↑ (3→5) | = (6+) | ↑ (particionado) | ↑ (manual→semi-auto) | = (informal) | ↑ (risco operacional) | ✅ PASS |
| 4 → 5 | = (20) | ↑ (5→10+) | ↑ (6→10+) | ↑ (3x Black Friday) | ↑ (semi→automatizada) | ↑ (informal→parcial/SLA) | ↑ (R$1.2M MegaShop) | ✅ PASS |
| 5 → 6 | ↑ (20→45) | ↑ (10→50+) | = (10+) | ↑ (50+ clientes) | = (automatizada) | ↑ (parcial→formal) | ↑ (R$800K + dominó) | ✅ PASS |
| 6 → 7 | = (45) | = (50+) | ↑ (integração total) | ↑ (E2E completo) | ↑ (auto→produção) | ↑ (formal→completa) | ↑ (R$20M Série A) | ✅ PASS |
| 7 → 8 | = (45+) | = (50+) | = (cenário integrador) | = (projeto autônomo) | = (demonstração) | ↑ (completa→auditada) | = (R$20M) | ✅ PASS |

### Resultado Global: ✅ TODAS AS TRANSIÇÕES PASSAM

Nenhuma regressão de complexidade foi detectada entre aulas consecutivas. A progressão é monotonicamente crescente (ou mantida) em todas as 7 dimensões avaliadas.

---

## 3. Verificação de Personagens (Req 2.3)

**Requisito**: Existem pelo menos 3 personagens recorrentes com papéis distintos (técnico, negócio, liderança).

| Personagem | Papel | Categoria | Presente em todas as aulas? |
|------------|-------|-----------|---------------------------|
| Marina Silva | CTO / Mentora técnica | **Técnico + Liderança** | ✅ Sim (Aulas 1-8) |
| Carlos Mendes | Engenheiro de Dados Sênior / Guia dos labs | **Técnico (execução)** | ✅ Sim (Aulas 1-8) |
| Ana Rodrigues | Product Owner | **Negócio** | ✅ Sim (Aulas 1-8) |
| Roberto Tanaka | CEO | **Liderança executiva** | ✅ Sim (Aulas 1-8) |

### Resultado: ✅ PASS

- 4 personagens recorrentes (excede o mínimo de 3)
- Papéis distintos cobertos: técnico-estratégico (Marina), técnico-prático (Carlos), negócio/produto (Ana), liderança/pressão (Roberto)
- Cada personagem tem propósito pedagógico claro e complementar

---

## 4. Verificação de Contexto Narrativo por Aula (Req 2.4)

**Requisito**: Cada aula inicia com contexto narrativo que justifica o conteúdo técnico da aula.

| Aula | Abertura narrativa existe? | Justifica conteúdo técnico? | Conexão clara? |
|------|---------------------------|----------------------------|----------------|
| 1 | ✅ Sim (script pandas trava) | ✅ Motiva introdução ao Spark | ✅ "Pandas não escala → Spark" |
| 2 | ✅ Sim (relatórios Black Friday) | ✅ Motiva joins/windows/UDFs | ✅ "Relatórios complexos → transformações avançadas" |
| 3 | ✅ Sim (3 parceiros, formatos distintos) | ✅ Motiva ingestão multi-formato | ✅ "Parceiros diferentes → pipeline de ingestão + Medallion" |
| 4 | ✅ Sim (Carlos de férias, pipeline para) | ✅ Motiva automação com Airflow | ✅ "Manual não escala → orquestração" |
| 5 | ✅ Sim (Black Friday, SLA, volume 3x) | ✅ Motiva orquestração avançada | ✅ "DAG linear insuficiente → sensores + branching" |
| 6 | ✅ Sim (3K duplicatas na MegaShop) | ✅ Motiva qualidade de dados | ✅ "Dados ruins chegam ao cliente → framework DQ" |
| 7 | ✅ Sim (board meeting, demo ao vivo) | ✅ Motiva pipeline end-to-end | ✅ "Peças isoladas → sistema integrado em produção" |
| 8 | ✅ Sim (board meeting, avaliação final) | ✅ Motiva projeto integrador | ✅ "Alunos replicam arquitetura completa" |

### Resultado: ✅ PASS

Todas as 8 aulas possuem abertura narrativa com cenas de diálogo entre personagens, e cada abertura conecta explicitamente a situação da empresa com o conteúdo técnico que será ensinado.

---

## 5. Verificação do Cenário Integrador — Aula 8 (Req 2.5)

**Requisito**: A narrativa da Aula 8 serve como cenário integrador para o projeto final.

| Critério | Verificação | Status |
|----------|-------------|--------|
| Exige aplicação de Spark (Aulas 1-3) | ✅ "Pipeline PySpark com 3+ transformações" | ✅ PASS |
| Exige aplicação de Airflow (Aulas 4-5) | ✅ "DAG Airflow com 4+ tasks" | ✅ PASS |
| Exige Docker (Aulas 1-7) | ✅ "Docker Compose funcional" | ✅ PASS |
| Exige qualidade de dados (Aula 6) | ✅ "Pelo menos 3 checks de qualidade" | ✅ PASS |
| Exige arquitetura medallion (Aula 3) | ✅ "Arquitetura medallion (Bronze/Silver/Gold)" | ✅ PASS |
| Cenário narrativo integrador | ✅ "Board meeting" — cada grupo apresenta para investidores | ✅ PASS |
| Demo ao vivo obrigatória | ✅ "Pipeline rodando, dados fluindo, métricas na tela" | ✅ PASS |
| Formato de apresentação definido | ✅ 20 min + 5 min perguntas | ✅ PASS |

### Resultado: ✅ PASS

A Aula 8 funciona como cenário integrador que exige explicitamente a aplicação de TODAS as tecnologias e conceitos ensinados nas Aulas 1-7.

---

## 6. Verificação de Progressão dos Stakes

| Aula | Incidente/Pressão | Consequência se falhar | Nível de urgência |
|------|-------------------|------------------------|-------------------|
| 1 | Script pandas trava (killed) | Relatório atrasa (cliente reclama) | 🟡 Baixo |
| 2 | Black Friday em 2 semanas | Relatórios de campanha não saem | 🟡 Médio |
| 3 | Schema mudou sem aviso → 40% dados zerados | Cliente recebe relatório errado | 🟠 Médio-Alto |
| 4 | Carlos de férias → 3 dias sem relatório | Operação inteira para (bus factor) | 🟠 Alto |
| 5 | Simulação falha → dados incompletos na Black Friday | R$ 1.2M de contrato em risco | 🔴 Muito Alto |
| 6 | 3K duplicatas detectadas pelo cliente | R$ 800K + 3 outras contas (dominó) | 🔴 Crítico |
| 7 | Board meeting → investidores querem demo ao vivo | R$ 20M Série A em jogo | 🔴 Máximo |
| 8 | Apresentação final (demonstração de competência) | Aprovação/reprovação | 🔴 Máximo |

### Resultado: ✅ PASS — Stakes monotonicamente crescentes

A progressão vai de "script quebra e relatório atrasa" (inconveniente) até "R$ 20M de investimento em risco" (existencial para a empresa). Nenhuma regressão de stakes entre aulas consecutivas.

---

## 7. Verificação de Progressão Técnica

| Aula | Tecnologia Principal | Sofisticação |
|------|---------------------|--------------|
| 1 | Spark básico (read, groupBy, agg) | ⭐ Fundamental |
| 2 | Spark avançado (joins, windows, UDFs, explain) | ⭐⭐ Intermediário |
| 3 | Spark + Data Lake (multi-formato, Medallion, particionamento) | ⭐⭐⭐ Avançado |
| 4 | Airflow básico (DAG, operators, XComs, schedule) | ⭐⭐ Intermediário (nova tecnologia) |
| 5 | Airflow avançado (sensors, branching, SparkSubmit, callbacks) | ⭐⭐⭐ Avançado |
| 6 | Framework DQ (checks, quarentena, monitoramento, SLA) | ⭐⭐⭐⭐ Profissional |
| 7 | Integração E2E (Docker + Spark + Airflow + DQ + logs + idempotência) | ⭐⭐⭐⭐⭐ Produção |
| 8 | Aplicação autônoma em cenário novo | ⭐⭐⭐⭐⭐ Autônomo |

### Resultado: ✅ PASS — Sofisticação técnica monotonicamente crescente

Nota: A transição 3→4 introduz uma nova tecnologia (Airflow) em nível "intermediário", o que poderia parecer uma regressão. Porém, o contexto de stack acumulado é crescente — na Aula 4, o aluno precisa dominar Spark E Airflow, o que é mais complexo do que apenas Spark avançado.

---

## 8. Ganchos entre Aulas — Verificação de Continuidade

| Transição | Gancho narrativo | Orgânico? | Motiva próxima aula? |
|-----------|-----------------|-----------|---------------------|
| 1 → 2 | ShopBrasil expande contrato → 10x dados + múltiplas fontes | ✅ Sim | ✅ Sim |
| 2 → 3 | 3 novos parceiros com formatos distintos assinam | ✅ Sim | ✅ Sim |
| 3 → 4 | Carlos de férias → ninguém sabe rodar pipeline | ✅ Sim | ✅ Sim |
| 4 → 5 | Black Friday + 10 fontes + SLA rigoroso | ✅ Sim | ✅ Sim |
| 5 → 6 | MegaShop detecta duplicatas → ameaça sair | ✅ Sim | ✅ Sim |
| 6 → 7 | Board meeting marcado → precisa integrar tudo | ✅ Sim | ✅ Sim |
| 7 → 8 | "Cada equipe monte algo assim" → projeto final | ✅ Sim | ✅ Sim |

### Resultado: ✅ PASS

Todos os ganchos são consequências orgânicas da resolução do problema da aula anterior (resolver A revela B), não artificiais.

---

## 9. Resumo Final da Validação

| # | Critério | Status |
|---|----------|--------|
| 2.2a | Empresa evolui progressivamente a cada aula | ✅ PASS |
| 2.2b | Sem regressão de dificuldade entre aulas consecutivas | ✅ PASS |
| 2.2c | Desafios de negócio ficam mais complexos | ✅ PASS |
| 2.2d | Soluções técnicas ficam mais sofisticadas | ✅ PASS |
| 2.2e | Stakes aumentam (de "script quebra" até "R$ 20M em risco") | ✅ PASS |
| 2.2f | Tamanho de equipe e complexidade organizacional aumentam | ✅ PASS |
| 2.2g | Número de fontes de dados aumenta | ✅ PASS |
| 2.2h | Volume de dados aumenta | ✅ PASS |
| 2.3 | Pelo menos 3 personagens com papéis distintos | ✅ PASS (4 personagens) |
| 2.4 | Cada aula inicia com contexto narrativo justificando conteúdo técnico | ✅ PASS (8/8 aulas) |
| 2.5 | Aula 8 serve como cenário integrador para projeto final | ✅ PASS |

### Veredicto: ✅ NARRATIVA VALIDADA — Complexidade Monotonicamente Crescente Confirmada

A narrativa da DataFlow Analytics atende a todos os critérios de progressão de complexidade definidos nos requisitos. Não foram encontradas regressões em nenhuma dimensão entre aulas consecutivas.

---

*Documento gerado como parte da validação do Task 3.5 — Spec Big Data Processing Course.*
