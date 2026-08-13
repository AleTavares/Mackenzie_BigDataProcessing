# Aberturas Narrativas — DataFlow Analytics

> **Instruções para o professor**: Cada texto abaixo é um roteiro de ~10 minutos para abrir a aula. Pode ser apresentado em 2-3 slides com os diálogos e contexto. Leia os diálogos com entonação (ou peça a alunos voluntários para interpretar). Encerre cada abertura conectando a situação ao conteúdo técnico que será ensinado.

---

## Aula 1 — O Limite do Pandas

### 🎬 Slide 1: Contexto da DataFlow

**Narração do professor:**

Vamos conhecer a DataFlow Analytics. Uma startup brasileira de análise de dados, fundada em São Paulo há 4 meses por Marina Silva — uma ex-Staff Engineer do Nubank — e Roberto Tanaka, um executivo com 15 anos de experiência no varejo digital.

A empresa acabou de fechar seu primeiro grande contrato: a **ShopBrasil**, uma rede de e-commerce com 200 lojas. O time tem 6 pessoas. O clima é de entusiasmo. Carlos Mendes, engenheiro de dados sênior, é responsável por processar os relatórios diários de vendas usando scripts Python com pandas.

Tudo funcionava bem... até a semana passada. O volume de dados cresceu. O script que antes levava 5 minutos para rodar agora leva mais de 4 horas. E nesta segunda-feira de manhã, Carlos chegou ao escritório e encontrou o seguinte no terminal:

> `Killed`

O processo foi encerrado pelo sistema operacional. Memória insuficiente. 100 mil registros. O pandas engasgou.

### 🎬 Slide 2: A Cena — Segunda-feira, 9h15

**👨‍🔧 Carlos** *(chegando ao escritório, olhando para o laptop com desespero)*:
> "Marina, o script morreu de novo. `Killed`. Cem mil registros e o pandas engasgou. Quatro horas e trinta e sete minutos rodando — e depois: morto. Sem cerimônia."

**👩‍💼 Ana** *(entrando apressada, celular na mão)*:
> "Gente, a ShopBrasil tá no telefone. O gerente comercial quer saber por que o relatório de ontem não chegou. É a terceira vez esse mês que a gente atrasa."

**👩‍💻 Marina** *(calma, levantando da mesa com o café na mão)*:
> "Carlos, deixa eu te fazer uma pergunta. Se o volume dobrar mês que vem — e vai dobrar, porque a ShopBrasil está em campanha de Black Friday — o que acontece?"

**👨‍🔧 Carlos**:
> "Acontece que o script não roda nem se eu comprar 64GB de RAM. É um problema de arquitetura, não de hardware."

**👩‍💻 Marina** *(desenhando no quadro branco)*:
> "Exatamente. Pandas roda em uma máquina. Memória finita. CPU finita. A solução não é uma máquina maior — é distribuir o trabalho entre várias máquinas. Já ouviram falar de Apache Spark?"

**👨‍🔧 Carlos** *(sorrindo)*:
> "Finalmente. Eu tava esperando você falar isso desde a semana passada."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

Essa situação que a DataFlow está vivendo é extremamente comum. Quantos de vocês já tiveram um script pandas que travou por falta de memória? Já tiveram que processar dados maiores do que cabiam na RAM?

Hoje vamos resolver exatamente esse problema. Vamos:

1. **Entender** por que ferramentas como pandas têm um limite de escala — e qual é esse limite
2. **Configurar** um ambiente Apache Spark com Docker (em 5 minutos vocês terão um cluster distribuído rodando no laptop)
3. **Executar** as mesmas operações que o Carlos fazia em pandas — mas agora com PySpark, de forma distribuída e escalável
4. **Comparar** a performance: quanto tempo pandas leva vs quanto tempo Spark leva para os mesmos 100K registros

No final desta aula, vocês vão ter resolvido o problema da DataFlow: o relatório da ShopBrasil vai sair em minutos, não em horas. E vão entender a arquitetura que permite isso.

> **💡 Pergunta para a turma:** Qual o maior dataset que vocês já processaram com pandas? O que aconteceu quando o volume cresceu?

---

## Aula 2 — Crescimento: 10x Mais Dados

### 🎬 Slide 1: Contexto — 4 Meses Depois

**Narração do professor:**

Lembram da DataFlow? Na Aula 1, resolvemos o problema básico: sair de pandas para Spark. O relatório diário agora roda em minutos. A ShopBrasil está satisfeita.

Tão satisfeita que **expandiu o contrato**. Agora são 1 milhão de registros de vendas. Mais 500 mil registros de clientes. E uma hierarquia de categorias em JSON. O volume cresceu 10x em 6 meses.

Mas o problema de hoje não é volume — é **complexidade**. Ana Rodrigues, a Product Owner, trouxe na reunião matinal os requisitos para a campanha de Black Friday da ShopBrasil. E o que ela precisa não se resolve com um simples `groupBy`.

### 🎬 Slide 2: A Cena — Daily Meeting, Terça-feira

**👩‍💼 Ana** *(projetando um dashboard na TV da sala)*:
> "Pessoal, a ShopBrasil precisa de três relatórios para a Black Friday. Primeiro: os **top 100 clientes por estado** — eles querem mandar cupons segmentados. Segundo: **tendência de compra mês a mês** por cliente — quem está comprando mais, quem está comprando menos. Terceiro: **segmentação automática** por faixa de ticket — baixo, médio, alto — para ajustar o pricing da campanha."

**👨‍🔧 Carlos** *(coçando a cabeça, pensativo)*:
> "Ok... o primeiro exige um join de vendas com clientes e depois um ranking particionado por estado. Isso é window function. O segundo precisa de lag temporal — comparar o mês atual com o anterior para cada cliente. E o terceiro... acho que vou precisar de uma UDF pra classificar por faixa."

**👨‍💼 Roberto** *(passando pela sala, café na mão, tom casual mas incisivo)*:
> "Duas semanas, pessoal. A Black Friday não espera. Quando isso vira relatório que eu posso mostrar pro cliente?"

**👩‍💻 Marina** *(levantando do canto da mesa)*:
> "Carlos, mostra pra gente como o Spark resolve joins distribuídos. Se o DataFrame de categorias é pequeno — uns 5 mil registros — podemos fazer broadcast e evitar shuffle. Vamos pensar em escala: o que funciona hoje com 1 milhão de registros precisa funcionar amanhã com 10 milhões."

**👨‍🔧 Carlos** *(abrindo o terminal, motivado)*:
> "Bora codar? Abre o notebook aí que a gente resolve isso em 10 minutos. Começo pelo join e a gente escala dali."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

O desafio de hoje é diferente da Aula 1. Não é mais sobre "fazer funcionar" — é sobre fazer funcionar **com inteligência e performance** quando múltiplas fontes de dados precisam ser combinadas.

Nesta aula, vocês vão aprender:

1. **Joins distribuídos** — inner, left, right, full, cross, semi e anti. Quando usar cada um e como o Spark otimiza internamente (broadcast joins para tabelas pequenas)
2. **Window Functions** — row_number, rank, dense_rank, lag, lead. Como calcular rankings, tendências e variações temporais sem perder performance
3. **UDFs (User Defined Functions)** — quando criar funções customizadas e qual o impacto na performance do Spark
4. **Plano de execução** — como ler o `explain()` e entender o que o Spark está realmente fazendo por baixo dos panos

No final, vocês vão entregar exatamente o que a Ana pediu: os três relatórios da Black Friday da ShopBrasil.

> **💡 Pergunta para a turma:** Vocês já tiveram que cruzar dados de 3+ tabelas diferentes? Qual foi a maior dificuldade — performance, lógica de negócio, ou ambos?

---

## Aula 3 — Expansão: Dados de Múltiplos Parceiros

### 🎬 Slide 1: Contexto — A DataFlow com 1 Ano

**Narração do professor:**

A DataFlow está fazendo 1 ano. A Black Friday foi um sucesso. Roberto está satisfeito — a ShopBrasil renovou por mais um ano. Mas com o sucesso veio o crescimento, e com o crescimento veio um novo tipo de caos.

A DataFlow agora tem **3 novos parceiros de dados**:
- A **PagFácil** — fintech que envia dados de pagamento em JSON via API
- O **MarketLog** — empresa de logística que deposita CSVs por SFTP, com encoding ISO-8859-1 e separador ponto-e-vírgula
- A **LogiExpress** — que tem um bucket com arquivos Parquet otimizados

Três parceiros. Três formatos completamente diferentes. Três schemas distintos. E Carlos tem um script separado para cada um — frágil, sem padrão, sem validação.

Na quinta-feira passada, aconteceu o inevitável.

### 🎬 Slide 2: A Cena — Reunião Emergencial, Sexta de Manhã

**👩‍💼 Ana** *(projetando e-mail do cliente na tela, tom urgente)*:
> "O relatório da ShopBrasil saiu com 40% dos valores zerados. A PagFácil mudou o nome de uma coluna — de `valor_total` para `total_venda` — sem avisar ninguém. O script de ingestão leu o campo antigo, não encontrou, e preencheu com zero. O cliente só percebeu na sexta de manhã."

**👨‍🔧 Carlos** *(suspirando, com cara de culpa)*:
> "Eu tenho um script separado pra cada parceiro. Eles são frágeis — qualquer mudança no schema e quebra sem aviso. Eu sei que não é o ideal, mas era o que dava pra fazer com o tempo que a gente tinha."

**👩‍💻 Marina** *(indo ao quadro branco, desenhando três caixas empilhadas)*:
> "Bronze. Silver. Gold. É assim que a gente resolve. Bronze é dado cru — exatamente como chegou, sem transformação. Silver é dado limpo e normalizado. Gold é dado pronto pro negócio. Se o parceiro mudar o schema, a Bronze grava mesmo assim — e a Silver pega o problema na validação, em vez de falhar silenciosamente."

**👨‍💼 Roberto** *(mensagem no Slack, que Marina lê em voz alta)*:
> "Marina — mais 2 parceiros assinaram ontem. Dados chegam em 2 semanas. Me diz que vocês estão prontos."

**👩‍💻 Marina** *(olhando para Carlos com determinação)*:
> "Precisamos de um pipeline de ingestão que funcione pra qualquer parceiro. Formato diferente, schema diferente, cadência diferente — mas saída padronizada. Vamos construir isso hoje."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

Esse cenário é o dia a dia de qualquer equipe de dados em empresa real. Vocês raramente vão ter o luxo de uma fonte única, formato perfeito e schema estável. Na prática, dados vêm de todo lugar, em todo formato, com toda inconsistência possível.

Nesta aula, vocês vão aprender:

1. **Ingestão multi-formato** — como o Spark lê CSV com encoding especial, JSON aninhado e Parquet otimizado, cada um com suas particularidades
2. **Arquitetura Medallion (Bronze/Silver/Gold)** — o padrão de data lake que separa dado cru de dado limpo de dado pronto para consumo
3. **Schema management** — schema inference vs schema enforcement, e como lidar quando parceiros mudam campos sem avisar
4. **Particionamento inteligente** — como organizar dados no disco para leitura eficiente em grande escala

Ao final do lab, vocês terão construído exatamente o que a Marina pediu: um pipeline que lê dados de 3 parceiros distintos, grava na Bronze sem perder nada, normaliza na Silver e disponibiliza na Gold.

> **💡 Pergunta para a turma:** Quantas fontes de dados diferentes vocês têm no trabalho atual? Estão todas padronizadas, ou cada uma tem seu jeitinho?

---

## Aula 4 — Maturidade Operacional: Automação com Airflow

### 🎬 Slide 1: Contexto — Carlos Foi de Férias

**Narração do professor:**

A DataFlow está com 1 ano e meio de operação. A arquitetura medallion está implementada. Os 5 parceiros estão integrados. Carlos está orgulhoso do trabalho — tão orgulhoso que resolveu tirar férias pela primeira vez em meses.

O problema? Carlos roda os 5 pipelines de ingestão **manualmente** toda manhã às 6h. É ele quem executa `python run_pipeline.py` na ordem certa, com os parâmetros certos, para cada parceiro. Ninguém mais no time sabe a sequência exata.

Na segunda-feira — primeiro dia sem Carlos — ninguém rodou os scripts. Na terça, alguém tentou e executou fora de ordem, corrompendo dados na Silver. Na quarta, Roberto descobriu que 3 dias de relatórios estavam atrasados.

Ana ligou para Carlos no meio das férias. Roberto mandou e-mail com o assunto: **"Bus factor = 1. Inaceitável."**

### 🎬 Slide 2: A Cena — Carlos Volta de Férias

**👨‍🔧 Carlos** *(voltando de férias, olhando o monitor com horror)*:
> "Marina... são 47 mensagens no Slack. Três dias sem relatório. Dois scripts rodaram fora de ordem e corromperam a Silver. Quem rodou os pipelines enquanto eu tava fora?"

**👩‍💻 Marina** *(calmamente, com tom de quem esperava esse momento)*:
> "Ninguém, Carlos. Esse é exatamente o ponto. A gente não pode ter um pipeline de produção que depende de um ser humano lembrar de rodar um comando todo dia às 6 da manhã. Se você for atropelado por um ônibus amanhã — desculpa a franqueza — a operação inteira para."

**👨‍💼 Roberto** *(entrando na sala, sem rodeios)*:
> "Carlos, bom ter você de volta. Mas vamos combinar uma coisa: nunca mais a operação inteira para porque alguém saiu de férias. Isso é inadmissível em uma empresa que quer escalar."

**👨‍🔧 Carlos** *(concordando, já querendo resolver)*:
> "Concordo 100%. Mas o que a gente usa? Cron? Task scheduler do Linux?"

**👩‍💻 Marina** *(balançando a cabeça)*:
> "Nenhum dos dois. Cron não tem retry automático, não tem dependência entre etapas, não tem visibilidade do que está acontecendo. Vamos usar Apache Airflow — um orquestrador de verdade. Você define o pipeline inteiro como código Python. Ele agenda, executa, retenta quando falha, e notifica quando termina. Tudo com interface visual."

**👨‍🔧 Carlos** *(aliviado, com humor)*:
> "Quem aqui já acordou às 6h da manhã pra rodar script na mão? Pois é. Nunca mais. Bora codar isso."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

Esse é um dos momentos mais importantes na maturidade de uma equipe de dados: sair do manual para o automatizado. Não importa quão bom é seu código de transformação se ele depende de alguém lembrar de rodar no horário certo.

Nesta aula, vocês vão aprender:

1. **Conceitos de orquestração** — o que é um orquestrador e por que cron não resolve
2. **Arquitetura do Airflow** — Webserver, Scheduler, Executor e Metadata Database
3. **DAGs como código** — como definir um pipeline inteiro em Python com dependências claras
4. **Operators** — PythonOperator para lógica customizada, BashOperator para comandos do sistema
5. **XComs** — como tasks se comunicam entre si (passar dados de uma etapa para outra)
6. **Schedule e retries** — como agendar execuções diárias com retry automático em caso de falha

No lab de hoje, vocês vão transformar o script manual do Carlos em uma DAG Airflow funcional. No final, o pipeline vai rodar sozinho todo dia às 6h — com ou sem Carlos no escritório.

> **💡 Pergunta para a turma:** Vocês têm pipelines que rodam manualmente no trabalho? O que acontece quando a pessoa responsável não está disponível?

---

## Aula 5 — Escala Corporativa: Orquestração Avançada

### 🎬 Slide 1: Contexto — Black Friday em 3 Semanas

**Narração do professor:**

A DataFlow está com 2 anos. As DAGs do Airflow estão rodando como relógio — o pipeline diário executa sozinho às 6h, Carlos finalmente pode dormir tranquilo.

Mas o cenário mudou. Roberto fechou contrato enterprise com a **MegaShop**, uma rede de 500 lojas. A DataFlow agora processa dados de **10 parceiros diferentes**. E a Black Friday está em 3 semanas.

O volume esperado na Black Friday é **3x o normal**. E a MegaShop impôs um SLA rigoroso: relatório pronto até 8h da manhã, todo dia, sem exceção. Se falhar uma vez durante a Black Friday... são R$ 1,2 milhão de receita em risco.

Na simulação de carga da semana passada, a DAG linear processou os dados do MarketLog antes deles chegarem — o arquivo só é depositado às 7h30, mas a DAG roda às 6h. Resultado: relatório incompleto. O diretor da MegaShop ligou pessoalmente para Roberto.

### 🎬 Slide 2: A Cena — War Room, Segunda-feira

**👨‍💼 Roberto** *(em pé na sala de reunião, tom sério, sem café na mão pela primeira vez)*:
> "Vou ser direto. A MegaShop nos deu SLA: relatório pronto até 8h da manhã, todo dia, sem exceção. Na Black Friday, o volume triplica. Se falhar UMA vez, são R$ 1,2 milhão de receita que vão pro concorrente. Não me importa qual ferramenta vocês usam. Me importa que funcione."

**👩‍💼 Ana** *(com planilha de simulação aberta)*:
> "Na simulação de ontem, o relatório saiu sem os dados do MarketLog. O arquivo deles só chega às 7h30 e nossa DAG já tinha rodado às 6h. Processamos 9 de 10 fontes — mas a que faltou representa 25% do volume."

**👩‍💻 Marina** *(no quadro, desenhando fluxo com bifurcações)*:
> "Precisamos de três coisas que a DAG linear de hoje não tem. Primeiro: **sensores** — o pipeline não pode rodar até confirmar que os dados chegaram. Segundo: **branching dinâmico** — na Black Friday, com 3x de volume, não faz sentido processar 50GB com Python puro. O Airflow precisa decidir automaticamente: volume grande vai pro Spark, volume pequeno vai pro Python. Terceiro: **alertas instantâneos** — se qualquer etapa falhar, quero notificação no Slack em 30 segundos."

**👨‍🔧 Carlos** *(traduzindo para ações concretas)*:
> "Então estamos falando de FileSensor pra esperar os arquivos, BranchPythonOperator pra decidir o caminho de processamento, SparkSubmitOperator pra rodar os jobs pesados, e callbacks de falha pra alertas. E vamos organizar isso com TaskGroups — senão 10 fontes com branching vira um espaguete impossível de manter."

**👩‍💻 Marina**:
> "Exatamente. Vamos montar essa arquitetura agora. A Black Friday não perdoa."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

A aula de hoje é sobre o que separa um pipeline "de brinquedo" de um pipeline de produção real. DAGs lineares funcionam para cenários simples — mas quando você tem 10 fontes, dependências complexas, volumes variáveis e SLAs apertados, precisa de orquestração inteligente.

Nesta aula, vocês vão aprender:

1. **FileSensor** — como fazer o pipeline esperar até que os dados realmente cheguem antes de processar
2. **BranchPythonOperator** — como criar lógica condicional que decide o fluxo de execução dinamicamente
3. **TaskGroups** — como organizar dezenas de tasks de forma visual e manutenível
4. **SparkSubmitOperator** — como integrar jobs Spark dentro do Airflow (o melhor dos dois mundos)
5. **Callbacks e alertas** — como configurar notificações automáticas quando algo dá errado
6. **trigger_rule** — como convergir branches e lidar com tasks que podem ou não executar

No lab, vocês vão construir a DAG que salva a DataFlow na Black Friday: sensor espera dados → branch decide caminho → Spark processa → alertas protegem.

> **💡 Pergunta para a turma:** Se o pipeline de vocês falhar às 3h da manhã, como vocês ficam sabendo? Quem é o "Carlos" que acorda pra resolver?

---

## Aula 6 — Governança: Qualidade de Dados

### 🎬 Slide 1: Contexto — O Incidente da MegaShop

**Narração do professor:**

A Black Friday foi um sucesso técnico. O pipeline processou 3x o volume normal. Os sensores seguraram a execução até todos os dados chegarem. O relatório da MegaShop foi entregue às 7h45 — 15 minutos antes do SLA. Roberto estava eufórico.

Até segunda-feira.

Na segunda-feira pós-Black Friday, Ana recebeu uma ligação gelada: a MegaShop detectou **3 mil pedidos duplicados** no relatório de sábado. O faturamento deles aparecia 15% maior do que o real. O diretor financeiro queria explicações imediatas.

O pipeline processou rápido. Processou no horário. Processou todas as fontes. Mas processou **dado ruim**. Duplicatas passaram direto do Bronze para o Gold sem ninguém perceber. Velocidade sem qualidade é perigoso.

Roberto escalou para nível máximo. A MegaShop tem cláusula contratual de penalidade por dados incorretos.

### 🎬 Slide 2: A Cena — Reunião de Crise

**👨‍💼 Roberto** *(projetando o e-mail formal da MegaShop na tela da sala)*:
> "Sessenta dias. É o que a MegaShop nos deu pra corrigir. Se o problema de dados duplicados não estiver resolvido até lá, eles exercem a cláusula 4.2 do contrato e migram para o DataStar. Estamos falando de R$ 800 mil de receita anual. E se a MegaShop sair, três outras contas menores vão junto. É um dominó."

**👩‍💼 Ana** *(com printscreen da planilha do cliente na tela)*:
> "Olha aqui: 3 mil pedidos aparecendo duas vezes. O faturamento de sábado está 15% inflado. O diretor financeiro deles está furioso — e com razão. Ele ia fechar o balanço mensal com esses números."

**👩‍💻 Marina** *(séria, de pé ao lado do quadro branco)*:
> "O problema é claro: nosso pipeline processa rápido, mas não valida. Nenhum check de qualidade entre as camadas. Dado duplicado entra na Bronze e chega no Gold intocado. A partir de hoje, **nenhum registro** chega na camada Gold sem passar por validação automática. Zero exceção."

**👨‍🔧 Carlos** *(já pensando na implementação)*:
> "O que precisamos? Check de unicidade pra pegar duplicatas. Check de completude pra nulls. Integridade referencial pra garantir que todo pedido tem cliente válido. E o que fazemos com dado ruim — descarta?"

**👩‍💻 Marina**:
> "Não descarta — **quarentena**. Dado inválido vai pra uma área separada com metadados do porquê foi rejeitado. A gente analisa depois, corrige se possível, e reprocessa. E se qualquer check CRÍTICO falhar acima do threshold, o pipeline PARA e manda alerta. Sem exceção."

**👨‍💼 Roberto** *(encerrando)*:
> "Marina, Carlos: 60 dias. O relógio tá correndo."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

Essa é a aula que separa engenheiros de dados juniores de seniores. Qualquer pessoa consegue fazer um pipeline que move dados do ponto A ao ponto B. Mas garantir que esses dados estão **corretos, completos e confiáveis**? Isso exige disciplina de engenharia.

Nesta aula, vocês vão aprender:

1. **Dimensões de qualidade de dados** — completude, unicidade, consistência, integridade referencial e timeliness
2. **Checks com PySpark** — como implementar validações programáticas em cada camada do data lake
3. **Sistema de quarentena** — como separar dados válidos de inválidos sem perder informação
4. **Monitoramento e alertas** — como integrar checks de qualidade na DAG Airflow com notificações automáticas
5. **Thresholds e SLAs** — quando um check deve gerar warning vs quando deve parar o pipeline

No lab, vocês vão construir o framework de qualidade que salva o contrato da MegaShop: checks de duplicata, completude e integridade, com quarentena automática para dados rejeitados.

> **💡 Pergunta para a turma:** Se eu perguntar agora quantas duplicatas existem no dado que vocês processam no trabalho... vocês saberiam responder? Se não, esse é exatamente o problema que vamos resolver.

---

## Aula 7 — Integração Total: Pipeline End-to-End

### 🎬 Slide 1: Contexto — Board Meeting em 4 Semanas

**Narração do professor:**

A DataFlow está prestes a completar 3 anos. A taxa de dados ruins caiu de 15% para 0,3%. A MegaShop renovou o contrato. Roberto está aliviado.

Mas agora Roberto olha para o próximo desafio: a **Série A**. Investidores querem ver a plataforma funcionando de verdade. Não slide bonito com diagramas de arquitetura — pipeline rodando ao vivo, dados entrando de um lado e relatório saindo do outro, com métricas na tela.

O board meeting foi marcado para daqui a 4 semanas. Demo ao vivo é obrigatória.

Marina olha para o cenário completo e percebe: todas as peças existem. Spark funciona. Airflow funciona. Qualidade funciona. Ingestão multi-formato funciona. Mas cada peça foi construída separadamente — notebooks aqui, scripts ali, DAGs de teste acolá. Nada está integrado como um **sistema de produção coeso**.

É hora de juntar tudo.

### 🎬 Slide 2: A Cena — Videoconferência com Roberto

**👨‍💼 Roberto** *(em videoconferência, tela compartilhada com o calendário)*:
> "Board meeting em 4 semanas. Os investidores querem ver o pipeline rodando — ao vivo. Não quero slide com diagrama bonito. Quero ver dado entrando de um lado e relatório saindo do outro. Com números reais na tela. Quem não entregar demo ao vivo, não está pronto pra Série A."

**👩‍💻 Marina** *(anotando, organizando mentalmente)*:
> "Então precisamos do pipeline completo em produção: ingestão de múltiplos parceiros, transformação com Spark, qualidade em cada camada, orquestração com Airflow, e entrega. Tudo containerizado em Docker, tudo automatizado, tudo com métricas visíveis."

**👨‍🔧 Carlos** *(pensativo, avaliando o escopo)*:
> "Marina, a gente tem cada peça separada. Ingestão funciona no notebook. Transformações funcionam no notebook. Airflow funciona com DAGs de teste. Qualidade funciona como script. O que falta é **grudar tudo** num sistema coeso. É como ter todas as peças do LEGO fora da caixa — agora precisa montar."

**👩‍💼 Ana** *(completando, com visão de produto)*:
> "E precisa ter critérios claros de sucesso pro board. Eu definiria: SLA de entrega até 8h, completude acima de 99,5%, zero duplicatas na camada Gold, e log de cada execução rastreável. Se a gente atingir isso ao vivo, os investidores vão se convencer."

**👩‍💻 Marina** *(delegando com clareza)*:
> "Carlos, você lidera a integração. Docker Compose com Spark, Airflow e monitoramento. Job Spark de produção com logging estruturado — não mais notebooks, mas scripts Python com argumentos CLI. DAG que orquestra Bronze → Silver → Gold. E **idempotência em cada etapa** — se precisar reprocessar um dia, não pode gerar duplicata. Esse é o ensaio geral."

**👨‍🔧 Carlos** *(sorrindo, determinado)*:
> "Bora montar esse LEGO. Começo pelo Docker Compose e subo camada por camada."

### 🎬 Slide 3: O Que Vamos Aprender Hoje

**Narração do professor:**

Esta é a aula de integração. Vocês já sabem cada peça individualmente: Spark, Airflow, qualidade, ingestão, Docker. Hoje vão juntar tudo em um **pipeline de produção real** — como o que roda em empresas como Nubank, iFood e Mercado Livre.

Nesta aula, vocês vão aprender:

1. **Spark jobs de produção** — transformar notebooks em scripts Python parametrizados com argumentos CLI e logging estruturado
2. **Idempotência** — como garantir que reprocessar um dia não gera duplicata (overwrite por partição)
3. **Docker Compose de produção** — configuração completa com Spark, Airflow e todos os serviços integrados
4. **Orquestração E2E** — DAG Airflow que orquestra o fluxo completo: ingestão → Bronze → Silver → Gold com checks de qualidade em cada transição
5. **Observabilidade** — logs, métricas e como saber que o pipeline está saudável

No lab, vocês vão construir o pipeline end-to-end da DataFlow: dados entram, passam por todas as camadas, são validados, e saem como relatório pronto — tudo automatizado, containerizado e observável.

> **💡 Pergunta para a turma:** Se alguém pedisse pra vocês mostrarem o pipeline de dados de vocês rodando ao vivo agora, vocês conseguiriam? O que falta?

---

## Aula 8 — Board Meeting: Projeto Final

### 🎬 Slide 1: Contexto — O Grande Dia

**Narração do professor:**

A DataFlow completa 3 anos. A empresa cresceu de 6 para 40 pessoas. Processa dados de 50+ clientes. Está prestes a levantar sua Série A.

O board meeting é **hoje**.

Mas aqui vai a virada narrativa: hoje, **vocês** são a DataFlow. Cada grupo recebeu um cenário de negócio de um vertical diferente — saúde, finanças, logística, varejo, agro. O desafio foi montar um pipeline completo usando tudo que aprendemos nas 7 aulas anteriores.

Hoje não é dia de aula teórica. Hoje é dia de **demonstração**. Cada grupo tem 20 minutos para apresentar sua solução — como se estivesse fazendo pitch para investidores de verdade.

O pipeline precisa rodar ao vivo. Não basta mostrar slides.

### 🎬 Slide 2: A Cena — Abertura do Board Meeting

**👨‍💼 Roberto** *(de pé na frente da sala, tela grande atrás dele)*:
> "Bom dia a todos. Vocês passaram 7 semanas construindo competência em Big Data. Spark. Airflow. Docker. Qualidade. Pipeline end-to-end. Hoje é o dia de mostrar que aprenderam de verdade. Não com teoria — com resultado."

**👩‍💻 Marina** *(ao lado de Roberto, com notebook aberto)*:
> "Cada grupo recebeu um cenário de negócio diferente. O desafio foi montar um pipeline completo — da ingestão ao relatório — usando tudo que aprendemos juntos. Eu vou avaliar a arquitetura técnica: Spark bem usado, DAG bem estruturada, qualidade implementada de verdade."

**👩‍💼 Ana** *(sentada à mesa de avaliação)*:
> "E eu vou avaliar se a solução resolve o problema de negócio. Não adianta ter código bonito se o cliente não recebe o que precisa. Me mostrem que vocês entendem o 'para quê' — não só o 'como'."

**👨‍💼 Roberto** *(encerrando a abertura)*:
> "Vinte minutos por grupo. Demo ao vivo obrigatória. Pipeline rodando, dados fluindo, métricas na tela. A tecnologia é meio, não fim — me mostrem o resultado de negócio. Primeiro grupo, podem começar."

### 🎬 Slide 3: Formato e Critérios

**Narração do professor:**

Antes de começarmos as apresentações, vamos alinhar o formato e os critérios de avaliação:

**Formato da apresentação (20 min + 5 min perguntas):**
- Contexto do problema de negócio (3 min)
- Arquitetura da solução (5 min)
- Demo ao vivo do pipeline rodando (7 min)
- Resultados e métricas (3 min)
- Lições aprendidas (2 min)

**Critérios de avaliação:**
- **Funcionamento** (30%) — O pipeline roda de ponta a ponta, ao vivo, sem erros?
- **Arquitetura** (25%) — Spark bem utilizado, DAG bem estruturada, Docker configurado corretamente?
- **Qualidade de dados** (15%) — Existem checks implementados? Dados inválidos são tratados?
- **Apresentação** (20%) — Clareza na comunicação, domínio do conteúdo, gestão do tempo?
- **Documentação** (10%) — README claro, código comentado, decisões justificadas?

**O que é obrigatório em cada projeto:**
- Pipeline PySpark com 3+ transformações
- DAG Airflow com 4+ tasks
- Docker Compose funcional
- Pelo menos 3 checks de qualidade
- Arquitetura medallion (Bronze/Silver/Gold)
- README documentando a solução

Boa sorte a todos. Lembrem: vocês passaram 7 semanas se preparando para esse momento. Confiem no que construíram.

> **💡 Nota final (quebrando a quarta parede):** Ao final das apresentações, lembrem que o Roberto vai dizer algo que vale para todos vocês: *"Vocês acabaram de fazer o que equipes de dados em empresas reais fazem todo dia: resolver problemas complexos, trabalhar sob pressão, entregar resultado com qualidade e defender decisões técnicas para stakeholders. A DataFlow é ficção — mas o que vocês construíram aqui é real. Levem isso pro próximo desafio."*

---

## Guia de Uso para o Professor

### Tempo de Apresentação

Cada abertura foi pensada para **10 minutos** de apresentação, distribuídos assim:
- **Slide 1** (3 min): Narração do contexto — professor conta o que aconteceu com a DataFlow
- **Slide 2** (4 min): Cena com diálogos — professor lê/interpreta as falas dos personagens
- **Slide 3** (3 min): Conexão com o conteúdo técnico — o que vamos aprender e por quê

### Dicas de Apresentação

1. **Dê vida aos personagens**: Use tons de voz diferentes para cada um. Roberto é direto e sério. Carlos é prático e bem-humorado. Ana é assertiva e focada no cliente. Marina é calma e estratégica.
2. **Pause para perguntas**: A pergunta final de cada abertura é para engajamento. Dê 1-2 minutos para a turma responder.
3. **Conecte com experiência real**: Após a cena, pergunte: "Quem aqui já viveu algo parecido no trabalho?" Isso cria ponte entre ficção e realidade.
4. **Use os slides como suporte visual**: Coloque uma imagem de escritório/reunião no fundo do Slide 2. Os diálogos podem aparecer como balões ou citações.
5. **Não leia tudo literalmente**: Adapte os diálogos ao seu estilo. O importante é manter a essência da cena e o problema que motiva o conteúdo.
