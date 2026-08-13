# Revisão de Consistência Narrativa — DataFlow Analytics

## Resumo Executivo

Este documento registra a revisão de consistência narrativa entre todas as 8 aulas do curso Big Data Processing. A revisão verificou: aparição dos personagens, consistência de nomes e papéis, progressão de complexidade, ganchos entre aulas e coerência geral da história da DataFlow Analytics.

**Resultado geral: ✅ Narrativa consistente com observações menores documentadas abaixo.**

---

## 1. Tabela de Aparição dos Personagens por Aula

| Personagem | Papel Canônico | Aula 1 | Aula 2 | Aula 3 | Aula 4 | Aula 5 | Aula 6 | Aula 7 | Aula 8 |
|-----------|---------------|--------|--------|--------|--------|--------|--------|--------|--------|
| **Marina Silva** | CTO | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Labs |
| **Carlos Mendes** | Eng. Dados Sênior | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Labs | ✅ Slides + Labs | ✅ Labs |
| **Ana Rodrigues** | Product Owner | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Labs (indireta) | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Labs | ✅ Labs |
| **Roberto Tanaka** | CEO | ✅ Slides | ✅ Slides | ✅ Slides | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Slides + Labs | ✅ Labs |

### Observações sobre aparições

- **Todos os 4 personagens aparecem em todas as 8 aulas** — conformidade total com Requisito 2.3.
- Marina e Carlos são os mais presentes (protagonistas técnicos dos labs).
- Roberto ganha progressivamente mais destaque a partir da Aula 5, culminando na Aula 8 como protagonista do board meeting.
- Ana aparece com menor destaque na Aula 4 (mencionada indiretamente — "Ana teve que ligar para Carlos"), mas está presente.
- Na Aula 8, não há slides teóricos (apenas materiais do projeto final nos labs), mas todos os 4 personagens aparecem no cronograma e especificação.

---

## 2. Consistência de Nomes e Papéis

### Nomes Canônicos (conforme `shared/narrativa/personagens.md`)

| Personagem | Nome Completo | Cargo |
|-----------|--------------|-------|
| Marina Silva | Marina Silva Santos | CTO e Co-fundadora |
| Carlos Mendes | Carlos Eduardo Mendes | Engenheiro de Dados Sênior |
| Ana Rodrigues | Ana Carolina Rodrigues | Product Owner |
| Roberto Tanaka | Roberto Hideki Tanaka | CEO e Co-fundador |

### Verificação de Consistência

| Item verificado | Resultado | Observações |
|----------------|-----------|-------------|
| Nome "Marina Silva" | ✅ Consistente | Sempre referida como "Marina Silva" ou "Marina" |
| Papel de Marina como CTO | ✅ Consistente | Sempre identificada como "(CTO)" em todas as aparições |
| Nome "Carlos Mendes" | ✅ Consistente | Sempre referido como "Carlos Mendes" ou "Carlos" |
| Papel de Carlos como Eng. Sênior | ✅ Consistente | Referido como "Engenheiro de Dados Sênior" ou "Eng. Sênior" |
| Nome "Ana Rodrigues" | ✅ Consistente | Sempre referida como "Ana Rodrigues" ou "Ana" |
| Papel de Ana como PO | ✅ Consistente | Referida como "Product Owner" ou "PO" |
| Nome "Roberto Tanaka" | ✅ Consistente | Sempre referido como "Roberto Tanaka" ou "Roberto" |
| Papel de Roberto como CEO | ✅ Consistente | Sempre identificado como "(CEO)" |

### Variações aceitáveis encontradas

- Carlos é referido como "Eng. Sênior" (abreviado) no design.md e em alguns datasets README — aceito como variação contextual.
- Ana é referida como "PO" (abreviação de Product Owner) em alguns contextos — variação aceita.

### ✅ Nenhuma inconsistência de nome ou papel encontrada.

---

## 3. Progressão de Complexidade (Crescimento Monotônico)

### Evolução validada

| Aula | Fase da Empresa | Complexidade Técnica | Complexidade Narrativa |
|------|----------------|---------------------|----------------------|
| 1 | Fundação (4 meses, 6 pessoas) | Pandas → Spark básico (100K registros) | Problema simples: script travou |
| 2 | Crescimento (8-10 meses) | Joins + Windows + UDFs (1M registros, 3 fontes) | Prazo apertado: Black Friday |
| 3 | Expansão (1 ano, 12 pessoas) | Multi-formato, Medallion, 3 parceiros (5M registros) | Schema quebra sem aviso |
| 4 | Maturidade (1.5 anos) | Automação com Airflow, DAGs, XComs | Bus factor = 1, férias de Carlos |
| 5 | Escala Corporativa (2 anos) | Sensors, Branching, SparkSubmit, 10 fontes | SLA de R$ 1.2M, Black Friday |
| 6 | Governança (2.5 anos, 50+ clientes) | Quality Framework, Quarentena, Alertas | Contrato em risco, compliance LGPD |
| 7 | Produção (quase 3 anos) | Pipeline E2E, Idempotência, Docker, Observabilidade | Board meeting em 4 semanas |
| 8 | Board Meeting (3 anos, 40 pessoas) | Integração de TUDO (projeto final) | Pitch para investidores |

### ✅ Complexidade monotonicamente crescente confirmada.

A empresa cresce consistentemente em:
- **Número de pessoas**: 6 → 12 → ~20 → 40
- **Número de clientes/parceiros**: 1 → 3 → 5 → 10 → 50+
- **Volume de dados**: 100K → 1M → 5M → multi-pipeline diário
- **Maturidade operacional**: Manual → Semi-auto → Automatizada → Produção
- **Governança**: Nenhuma → Informal → Parcial → Formal → Auditada

---

## 4. Ganchos entre Aulas

| Transição | Gancho (como a aula atual conecta com a próxima) | Presente nos Slides? | Coerente? |
|-----------|---------------------------------------------------|---------------------|-----------|
| **1 → 2** | ShopBrasil expande contrato, 10x mais dados, múltiplas fontes para cruzar | ✅ Slide 36 "Próxima Aula" | ✅ |
| **2 → 3** | 3 novos parceiros com formatos diferentes (CSV, JSON, Parquet) | ✅ Slide 37 "Próxima Aula" | ✅ |
| **3 → 4** | Carlos roda pipelines manualmente às 6h — insustentável com novos parceiros | ✅ Slide 39 "Próxima Aula" | ✅ |
| **4 → 5** | 10 fontes com dependências complexas, Black Friday com SLA apertado | ✅ Slide 38 "Próxima Aula" | ✅ |
| **5 → 6** | MegaShop detecta duplicatas, compliance LGPD, dados ruins chegam ao cliente | ✅ Slide 38 "Preview Aula 6" | ✅ |
| **6 → 7** | Peças existem isoladamente, precisam ser integradas em pipeline de produção | ✅ Último slide "Próxima aula" | ✅ |
| **7 → 8** | Roberto cobra demo ao vivo, cada equipe monta pipeline para vertical diferente | ✅ "Próxima Aula: Projeto Final" | ✅ |

### ✅ Todos os ganchos estão presentes e coerentes entre si.

---

## 5. Coerência da História DataFlow Analytics

### Verificação de continuidade

| Aspecto | Status | Detalhes |
|---------|--------|----------|
| Cliente principal (ShopBrasil) | ✅ Coerente | Aparece em Aulas 1-3, depois referido indiretamente |
| Cliente enterprise (MegaShop) | ✅ Coerente | Introduzido na Aula 5, crise na Aula 6, renovação na Aula 7 |
| Parceiros de dados (PagFácil, MarketLog, LogiExpress) | ✅ Coerente | Introduzidos na Aula 3, mantidos depois |
| Arquitetura Medallion | ✅ Coerente | Introduzida na Aula 3, reutilizada nas Aulas 4-7 e projeto final |
| Docker Compose evolutivo | ✅ Coerente | Spark (Aula 1) → +Airflow (Aula 4) → Full (Aula 7) |
| Série A / Investidores | ✅ Coerente | Mencionados na Aula 7, culminam na Aula 8 |
| Fundação da empresa em 2021 | ✅ Coerente | Mantido em todos os materiais |
| Localização São Paulo | ✅ Coerente | Não há contradições |

---

## 6. Narrativa "Board Meeting" da Aula 8

### Verificação de culminação do arco

| Elemento | Presente? | Detalhes |
|----------|-----------|----------|
| Roberto como protagonista/presidente do board | ✅ | Abre o board meeting, avalia apresentações |
| Marina como avaliadora técnica | ✅ | Avalia arquitetura e decisões de engenharia |
| Ana como "voz do cliente" | ✅ | Avalia aderência ao problema de negócio |
| Carlos como orientador técnico | ✅ | Orienta grupos durante preparação |
| Referência explícita às 7 aulas anteriores | ✅ | "7 semanas construindo competência" |
| Demo ao vivo obrigatória | ✅ | "Pipeline rodando, dados fluindo, métricas na tela" |
| Conexão com Série A | ✅ | "Quem não entregar, não está pronto para a Série A" |
| Formato pitch para investidores | ✅ | Grupos apresentam como se fossem equipes da DataFlow |

### ✅ A Aula 8 culmina corretamente o arco narrativo completo.

---

## 7. Observações Menores (Não-Inconsistências)

### 7.1 Variação narrativa na Aula 6 (slides vs abertura narrativa)

**Observação**: Os slides da Aula 6 mencionam o problema como "ShopBrasil com faturamento divergente em 12%" e "auditoria LGPD em 60 dias", enquanto a abertura narrativa canônica (`aberturas_narrativas.md`) e o roteiro completo referem o incidente como "MegaShop com 3 mil pedidos duplicados" e "cláusula contratual de 60 dias".

**Análise**: Isto é uma **simplificação/adaptação** para o contexto dos slides (que devem ser concisos), não uma contradição fundamental. Ambos tratam do mesmo tema (dados incorretos chegando ao cliente, necessidade de qualidade). O conteúdo dos labs (exercício 02_check_unicidade, exercício 05_dag_qualidade) usa corretamente "MegaShop" e "duplicatas" em linha com o roteiro canônico.

**Impacto**: Baixo. O professor pode usar qualquer das duas versões na abertura oral (10 min) e depois convergir para o conteúdo do lab. Recomenda-se, em revisão futura, alinhar o slide 2 da Aula 6 com a versão MegaShop para máxima coerência.

### 7.2 Aula 8 sem arquivo de slides

**Observação**: A pasta `aula_08/slides/` contém apenas `.gitkeep` (sem slides teóricos). Isto é **correto e intencional** — a Aula 8 é dedicada inteiramente a apresentações do projeto final e não possui conteúdo teórico novo (conforme Requisito 9.7).

### 7.3 Avatares/Emojis dos personagens

| Personagem | Emoji nos slides | Emoji na abertura narrativa | Documento de personagens |
|-----------|-----------------|---------------------------|-------------------------|
| Marina | — | 👩‍💻 | 👩‍💻 |
| Carlos | — | 👨‍🔧 | 👨‍🔧 |
| Ana | — | 👩‍💼 | 👩‍💼 |
| Roberto | — | 👨‍💼 | 👨‍💼 |

Emojis usados consistentemente nos materiais narrativos.

---

## 8. Conclusão

A revisão completa de consistência narrativa entre as 8 aulas do curso confirma:

1. ✅ **Personagens consistentes** — Os 4 personagens (Marina, Carlos, Ana, Roberto) aparecem em todas as aulas com nomes e papéis corretos, sem erros de ortografia ou atribuição.

2. ✅ **Complexidade monotonicamente crescente** — A empresa DataFlow cresce progressivamente em volume de dados, número de clientes, maturidade operacional e governança, sem regressões.

3. ✅ **Ganchos entre aulas presentes** — Cada aula possui seção "Próxima Aula" nos slides e a abertura narrativa canônica conecta diretamente com o problema técnico da aula seguinte.

4. ✅ **Arco narrativo coerente** — A história da DataFlow não apresenta contradições fundamentais. O board meeting da Aula 8 culmina adequadamente o arco de 7 semanas de crescimento.

5. ⚠️ **Observação menor** — O slide 2 da Aula 6 usa uma variação do incidente (ShopBrasil/12%) diferente da versão canônica (MegaShop/3K duplicatas). Não é uma contradição grave, mas pode ser alinhada em revisão futura.

---

*Documento gerado em revisão de tarefa 12.1 — Revisão de Consistência Narrativa*
*Verificados: 7 arquivos de slides, 40+ arquivos de lab, 4 documentos narrativos de referência*
