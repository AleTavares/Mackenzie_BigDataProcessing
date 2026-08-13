# Personagens Recorrentes — DataFlow Analytics

Este documento descreve os 4 personagens ficcionais que aparecem ao longo das 8 aulas do curso Big Data Processing. Cada personagem serve a um propósito pedagógico distinto e aparece de forma consistente na narrativa, criando familiaridade e engajamento.

---

## 👩‍💻 Marina Silva — CTO e Mentora Técnica

### Dados Pessoais

| Campo | Valor |
|-------|-------|
| **Nome completo** | Marina Silva Santos |
| **Idade** | 38 anos |
| **Cargo** | CTO e Co-fundadora |
| **Avatar** | 👩‍💻 |
| **Localização** | São Paulo, SP (nasceu em Campinas) |
| **Formação** | Eng. da Computação (Unicamp) + Mestrado em Sistemas Distribuídos (USP) |

### Bio Profissional

Marina começou sua carreira como desenvolvedora back-end na TOTVS em 2009, onde aprendeu a lidar com sistemas legados e grandes volumes de dados corporativos. Após 3 anos, migrou para o iFood no início da operação, onde liderou a construção da primeira plataforma de dados da empresa — experiência que a marcou profundamente. Em 2016 foi contratada pelo Nubank como Staff Engineer na área de Data Platform, onde trabalhou com Apache Spark, Kafka e arquiteturas event-driven em escala. Publicou artigos sobre processamento distribuído no blog técnico do Nubank e palestrou em eventos como QCon SP e Data Hackers Conference. Em 2021, deixou a segurança do Nubank para co-fundar a DataFlow Analytics com Roberto Tanaka, motivada pela visão de democratizar análise de dados para o varejo brasileiro. Sua bagagem técnica de 12 anos em empresas de ponta é o que dá confiança ao time — e aos investidores — de que a migração tecnológica vai dar certo.

### Personalidade

- **Visionária pragmática**: Pensa no longo prazo, mas sabe priorizar o que resolver primeiro
- **Didática por natureza**: Explica conceitos complexos com analogias do cotidiano
- **Exigente com qualidade**: Não aceita gambiarras — prefere investir tempo agora para não pagar depois
- **Calma sob pressão**: Quanto maior a crise, mais focada e metódica ela fica

### Estilo de Comunicação

Marina fala com autoridade técnica, mas sem arrogância. Usa analogias para explicar decisões arquiteturais ("Spark é como ter 10 cozinheiros trabalhando na mesma receita em paralelo — em vez de um cozinheiro fazendo tudo sozinho"). Gosta de desenhar diagramas no quadro branco. Faz perguntas socráticas antes de dar respostas ("O que acontece se esse volume dobrar mês que vem?"). Raramente levanta a voz, mas quando diz "precisamos conversar sobre isso", todo mundo para o que está fazendo.

### Bordão

> "Vamos pensar em escala. O que funciona hoje precisa funcionar com 10x mais dados amanhã."

### Papel no Curso

| Aula | Atuação de Marina |
|------|-------------------|
| 1 | Apresenta o problema: pandas não escala mais. Decide adotar Spark. |
| 2 | Explica por que transformações complexas exigem processamento distribuído. |
| 3 | Desenha a arquitetura Medallion (Bronze/Silver/Gold) no quadro. |
| 4 | Determina que é hora de parar de rodar scripts na mão — apresenta Airflow. |
| 5 | Exige orquestração robusta com sensores e branching para a Black Friday. |
| 6 | Institui programa de qualidade de dados após incidente com cliente premium. |
| 7 | Valida a arquitetura end-to-end e aprova para produção. |
| 8 | Faz o papel de avaliadora técnica nas apresentações do projeto final. |

**Propósito pedagógico**: Marina conecta teoria à prática. Ela explica o **porquê** das decisões técnicas — por que Spark e não pandas, por que Airflow e não cron, por que qualidade de dados importa. Contextualiza cada tecnologia dentro de uma necessidade real de engenharia.

### Falas Típicas

> "Pessoal, ontem nosso script de vendas levou 4 horas pra rodar. Com Spark distribuído, o mesmo processamento leva 12 minutos. Não é mágica — é arquitetura."

> "Não estou pedindo perfeição no primeiro dia. Estou pedindo que a gente pare de apagar incêndio e comece a construir algo que aguente o tranco."

> "Se a gente não resolver isso antes da Black Friday, vamos ter que escolher quais clientes atender e quais vão ficar sem relatório. Essa não é uma escolha que eu quero fazer."

---

## 👨‍🔧 Carlos Mendes — Engenheiro de Dados Sênior

### Dados Pessoais

| Campo | Valor |
|-------|-------|
| **Nome completo** | Carlos Eduardo Mendes |
| **Idade** | 33 anos |
| **Cargo** | Engenheiro de Dados Sênior / Tech Lead dos Squads |
| **Avatar** | 👨‍🔧 |
| **Localização** | São Paulo, SP (nasceu em Belo Horizonte) |
| **Formação** | Ciência da Computação (UFMG) + Especialização em Big Data (FIAP) |

### Bio Profissional

Carlos começou programando em Java e SQL no Banco Inter em BH, onde descobriu sua paixão por dados ao automatizar relatórios regulatórios que antes levavam uma semana para ficar prontos. Em 2017 mudou para São Paulo e ingressou na Loggi como Engenheiro de Dados, onde trabalhou pela primeira vez com Apache Spark para otimizar rotas de entrega processando milhões de registros de GPS por dia. Na Loggi, aprendeu na prática o que funciona e o que quebra em pipelines de produção — experiência que o tornou metódico e cauteloso. Em 2019 passou pela Creditas, onde implementou pipelines Airflow para automação de processos de crédito imobiliário. Foi contratado por Marina em 2022 como o primeiro engenheiro sênior da DataFlow, com a missão de profissionalizar a infraestrutura de dados. Carlos é o tipo de engenheiro que documenta tudo, testa antes de subir pra produção e tem sempre um script pronto no bolso para resolver problemas. O time o considera a "cola" técnica que mantém tudo funcionando.

### Personalidade

- **Metódico e detalhista**: Segue processos step-by-step e documenta cada decisão
- **Mão na massa**: Prefere mostrar fazendo do que ficar em reunião discutindo
- **Paciente como professor**: Explica conceitos quantas vezes for necessário, sem irritação
- **Humor seco**: Faz piadas curtas e certeiras, especialmente sobre bugs em produção

### Estilo de Comunicação

Carlos é direto e prático. Comunica-se através de código e demonstrações ("Deixa eu abrir o terminal aqui que fica mais fácil de entender"). Usa muitos exemplos concretos e evita abstração excessiva. Quando algo dá errado, seu primeiro instinto é olhar os logs. Gosta de analogias com receitas de cozinha e montagem de LEGO. Em reuniões, costuma falar pouco até ter algo concreto para demonstrar.

### Bordão

> "Bora codar? Abre o terminal aí que a gente resolve isso em 10 minutos."

### Papel no Curso

| Aula | Atuação de Carlos |
|------|-------------------|
| 1 | Guia o lab de introdução ao Spark — primeiro DataFrame, primeiras operações. |
| 2 | Demonstra joins e window functions com dados reais dos clientes. |
| 3 | Implementa a ingestão multi-formato ao vivo (CSV, JSON, Parquet). |
| 4 | Transforma o script manual em DAG Airflow passo a passo. |
| 5 | Mostra branching, sensores e integração Spark + Airflow. |
| 6 | Constrói o framework de qualidade de dados com checks e quarentena. |
| 7 | Monta o pipeline end-to-end completo — do Docker ao Spark ao Airflow. |
| 8 | Atua como orientador técnico dos grupos durante apresentações. |

**Propósito pedagógico**: Carlos é o guia prático. Ele explica o **como** — como configurar, como escrever o código, como debugar quando quebra. É a voz que acompanha os alunos nos labs, dando instruções claras e troubleshooting.

### Falas Típicas

> "Calma, esse erro aí eu já vi umas 50 vezes. Faltou um `.master('local[*]')` na SparkSession. Cola isso aqui e roda de novo."

> "Olha, eu sei que parece muito código. Mas se você entender que cada bloco faz UMA coisa, fica simples. Primeiro lê, depois transforma, depois escreve. ETL clássico."

> "Quem aqui já acordou às 6h da manhã pra rodar script na mão? Pois é. Nunca mais. Airflow resolve isso com 20 linhas de Python."

---

## 👩‍💼 Ana Rodrigues — Product Owner

### Dados Pessoais

| Campo | Valor |
|-------|-------|
| **Nome completo** | Ana Carolina Rodrigues |
| **Idade** | 29 anos |
| **Cargo** | Product Owner — Plataforma de Dados |
| **Avatar** | 👩‍💼 |
| **Localização** | São Paulo, SP (nasceu em Recife) |
| **Formação** | Administração (UFPE) + Pós-graduação em Product Management (PM3) |

### Bio Profissional

Ana começou sua carreira como trainee na Ambev em Recife, onde desenvolveu forte visão analítica ao otimizar processos logísticos de distribuição no Nordeste. Após 2 anos, migrou para o ecossistema de startups ao ingressar na VTEX (plataforma de e-commerce) como Analista de Produto, onde aprendeu profundamente as dores do varejo digital brasileiro e se apaixonou por dados como ferramenta de decisão. Em 2020, se mudou para São Paulo e trabalhou na RD Station como Product Manager de Analytics, responsável por traduzir necessidades de negócio em requisitos técnicos para o time de dados. Foi nessa posição que desenvolveu sua habilidade principal: ser ponte entre mundos — falar "tecniquês" com engenheiros e "negocês" com executivos. Ingressou na DataFlow em 2022, atraída pela proposta de construir um produto de dados do zero. Ana é quem garante que toda decisão técnica tenha justificativa de negócio e que todo feature entregue resolva uma dor real dos clientes.

### Personalidade

- **Orientada a resultados**: Sempre pergunta "qual o impacto disso pro cliente?"
- **Comunicativa e empática**: Sabe ouvir e traduzir necessidades entre áreas
- **Organizada e data-driven**: Prioriza com base em dados, não em opiniões
- **Assertiva sem ser agressiva**: Defende posições com argumentos sólidos, mas acolhe contrapontos

### Estilo de Comunicação

Ana fala a linguagem do negócio e traduz para o time técnico. Usa métricas, exemplos de clientes e cenários de uso. Gosta de user stories e critérios de aceite claros. Em reuniões, costuma trazer print de reclamações de clientes para justificar prioridades. Faz muitas perguntas ("Quem é impactado?", "Qual o custo de não fazer?", "Quando o cliente precisa disso?"). Usa quadros Kanban e roadmaps como ferramentas de comunicação.

### Bordão

> "Legal a solução técnica, mas me conta: qual problema do cliente isso resolve?"

### Papel no Curso

| Aula | Atuação de Ana |
|------|----------------|
| 1 | Traz o requisito: "Precisamos processar 100K vendas por dia sem atrasar o relatório." |
| 2 | Pede relatório cruzando vendas + clientes + categorias para campanha de marketing. |
| 3 | Comunica que 3 novos parceiros entraram — cada um com formato diferente de dados. |
| 4 | Reclama que relatórios atrasam quando Carlos está de férias — exige automação. |
| 5 | Demanda que relatórios só saiam após TODAS as fontes serem processadas com sucesso. |
| 6 | Reporta que a MegaShop (cliente premium) recebeu dados duplicados — precisa de QA. |
| 7 | Define critérios de aceite para o pipeline de produção (SLA, completude, freshness). |
| 8 | Representa a "voz do cliente" durante avaliação dos projetos finais. |

**Propósito pedagógico**: Ana contextualiza o **o quê** e o **para quem**. Ela traz os requisitos de negócio que motivam cada solução técnica. Mostra aos alunos que engenharia de dados não existe em vácuo — existe para resolver problemas de pessoas reais.

### Falas Típicas

> "Gente, a MegaShop ligou de novo. O relatório de ontem veio com 3 mil pedidos duplicados. Isso é 15% do faturamento deles aparecendo dobrado. O diretor comercial deles está furioso."

> "Eu sei que vocês querem refatorar o pipeline todo, mas o cliente precisa do relatório de Black Friday semana que vem. O que a gente consegue entregar até sexta com segurança?"

> "Esse novo parceiro vai mandar dados em JSON via API. Outro manda CSV por SFTP. E o terceiro tem um bucket S3 com Parquet. Vocês conseguem normalizar tudo isso numa tabela única?"

---

## 👨‍💼 Roberto Tanaka — CEO

### Dados Pessoais

| Campo | Valor |
|-------|-------|
| **Nome completo** | Roberto Hideki Tanaka |
| **Idade** | 44 anos |
| **Cargo** | CEO e Co-fundador |
| **Avatar** | 👨‍💼 |
| **Localização** | São Paulo, SP (nasceu em Londrina, PR — família de origem japonesa) |
| **Formação** | Administração de Empresas (FGV-SP) + MBA em Gestão de Tecnologia (Insper) |

### Bio Profissional

Roberto iniciou sua carreira em 2003 no programa de trainee da B2W Digital (atual Americanas S.A.), onde passou 7 anos escalando posições até chegar a Gerente de Operações de E-commerce, responsável por uma operação de R$ 2 bilhões em vendas anuais. Em 2010, migrou para consultoria na McKinsey & Company, onde liderou projetos de transformação digital para varejistas brasileiros durante 4 anos — experiência que lhe deu visão estratégica e capacidade de dialogar com C-levels. Em 2014, foi contratado como VP de Estratégia Digital na Via Varejo (Casas Bahia/Ponto), onde liderou a digitalização de processos logísticos. Após 6 anos no mundo corporativo, percebeu que a inovação real acontecia nas startups. Em 2020, conheceu Marina num meetup de dados e, em 2021, co-fundaram a DataFlow Analytics. Roberto traz o entendimento profundo do varejo brasileiro, a rede de contatos com executivos de e-commerce e a experiência de escalar operações — mas depende de Marina para todas as decisões técnicas.

### Personalidade

- **Orientado a números e prazos**: Pensa em receita, runway, churn e ROI
- **Direto e sem rodeios**: Vai ao ponto rapidamente, não tolera enrolação
- **Estratégico e competitivo**: Sempre pensando no mercado e nos concorrentes
- **Justo, mas exigente**: Dá autonomia ao time, mas cobra resultados concretos

### Estilo de Comunicação

Roberto fala a linguagem de negócios e finanças. Usa termos como "runway", "burn rate", "CAC", "churn" naturalmente. Em reuniões técnicas, pede traduções para impacto em negócio ("Ok, mas isso reduz quanto do churn?"). Faz perguntas incômodas com naturalidade ("Se a gente não resolver até dezembro, o que acontece?"). Manda mensagens curtas no Slack, geralmente com urgência implícita. Nas raras vezes que elogia, o peso é enorme — o time valoriza cada reconhecimento.

### Bordão

> "Resultado. Me mostra o resultado. A tecnologia é meio, não fim."

### Papel no Curso

| Aula | Atuação de Roberto |
|------|---------------------|
| 1 | Mencionado indiretamente — foi ele quem autorizou o investimento em Spark. |
| 2 | Pergunta em reunião de status: "Quando isso vira relatório pro cliente?" |
| 3 | Comunica que novos parceiros assinaram contrato — dados chegam em 2 semanas. |
| 4 | Cobra que pipelines rodem sem depender de pessoas específicas (bus factor). |
| 5 | Exige SLA: "Relatório precisa estar pronto até 8h, todo dia, sem exceção." |
| 6 | Escala o incidente MegaShop: "Se perdermos esse cliente, são R$ 800K de receita." |
| 7 | Aprova orçamento de infraestrutura para produção — quer ver ROI em 90 dias. |
| 8 | **Protagonista**: Faz papel de "board member" avaliando apresentações dos grupos. |

**Propósito pedagógico**: Roberto traz a pressão do **quando** e do **quanto**. Ele representa o mundo real onde tecnologia precisa gerar resultado de negócio, com prazos e orçamentos. Na Aula 8, sua presença como "avaliador executivo" simula a experiência de apresentar resultados técnicos para stakeholders não-técnicos.

### Falas Típicas

> "Marina, eu confio em você. Mas os investidores querem ver progresso concreto. Manda um antes-e-depois: quanto tempo levava com pandas, quanto tempo leva com Spark. Números."

> "Sessenta dias. É o que a MegaShop nos deu. Se o problema de dados duplicados não estiver resolvido até lá, eles migram pro DataStar. E levam outras três contas junto."

> "Não me importa qual ferramenta vocês usam. Me importa que o relatório esteja na caixa do cliente toda manhã às 8h, com dados corretos. Zero erro. Conseguem?"

---

## Dinâmica entre Personagens

### Mapa de Relacionamentos

```
    Marina (CTO)                    Roberto (CEO)
   👩‍💻 Técnica                     👨‍💼 Negócio
       │                                │
       │ confiança                      │ pressão
       │ mútua                          │ construtiva
       │                                │
       ├─────── Co-fundadores ──────────┤
       │                                │
       │ mentora                        │ cobra
       ▼                                ▼
    Carlos (Eng. Sênior)            Ana (PO)
   👨‍🔧 Execução                    👩‍💼 Produto
       │                                │
       │ implementa                     │ prioriza
       │ soluções                       │ demandas
       │                                │
       └──────── Colaboram ─────────────┘
              diariamente
```

### Interações Típicas

| Interação | Contexto |
|-----------|----------|
| Marina → Carlos | "Carlos, preciso que você monte um POC de Spark para o relatório de vendas. Faz um benchmark contra o script pandas atual." |
| Ana → Carlos | "Carlos, o cliente precisa de um relatório novo cruzando vendas com devoluções. Dá pra ter até sexta?" |
| Roberto → Marina | "Marina, quanto custa essa migração pro Spark? Preciso colocar no board deck da próxima semana." |
| Ana → Roberto | "Roberto, se a gente não priorizar qualidade de dados agora, vamos perder mais dois clientes até o fim do trimestre." |
| Carlos → Marina | "Marina, achei um gargalo no join — se a gente fizer broadcast do DataFrame menor, corta o tempo pela metade." |
| Roberto → Ana | "Ana, quero um relatório semanal de satisfação dos clientes com a qualidade dos dados. KPI novo." |

### Complementaridade Pedagógica

| Pergunta | Quem Responde |
|----------|---------------|
| "Por que estamos fazendo isso?" | Marina (visão técnica estratégica) |
| "Como fazemos isso na prática?" | Carlos (implementação hands-on) |
| "O que o cliente precisa?" | Ana (requisitos de negócio) |
| "Quando precisa estar pronto?" | Roberto (prazos e resultado) |

---

## Guia de Uso para Materiais do Curso

### Nos Slides (Teoria)

- **Marina** aparece nos slides de contexto e decisões arquiteturais
- **Ana** aparece em slides de requisitos e motivação de negócio
- **Roberto** aparece quando há pressão por prazo ou resultado financeiro

### Nos Labs (Prática)

- **Carlos** é a voz principal — guia passo a passo, dá dicas e resolve erros
- **Marina** aparece em caixas "💡 Insight da Marina" com explicações conceituais

### Nas Aberturas Narrativas (10 min por aula)

- Todos os 4 personagens participam de uma "cena" que contextualiza o desafio da aula
- Formato: diálogo curto (4-6 falas) que termina com a motivação para o conteúdo técnico

### Na Aula 8 (Projeto Final)

- **Roberto** é o avaliador principal (simula board meeting)
- **Marina** avalia aspectos técnicos
- **Ana** avalia aderência ao problema de negócio
- **Carlos** orienta tecnicamente durante preparação

---

*Este documento deve ser consultado ao criar qualquer material do curso que inclua os personagens, garantindo consistência de personalidade, fala e papel pedagógico.*
