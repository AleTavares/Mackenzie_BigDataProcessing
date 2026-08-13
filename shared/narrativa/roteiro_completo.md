# Roteiro Narrativo Completo — DataFlow Analytics

## Visão Geral do Arco Narrativo

Este documento descreve a evolução da história da **DataFlow Analytics** ao longo das 8 aulas do curso Big Data Processing. A narrativa acompanha o crescimento da empresa — de uma startup com seus primeiros clientes até uma empresa madura apresentando resultados ao conselho — criando contexto emocional e profissional para cada desafio técnico.

### Arco em Uma Frase
> De um script pandas travando no laptop da CTO até um pipeline de produção apresentado ao board — a jornada técnica da DataFlow Analytics espelha a jornada de aprendizado dos alunos.

### Progressão de Complexidade

| Aula | Fase da Empresa | Nº Fontes | Volume Dados | Automação | Governança |
|------|----------------|-----------|--------------|-----------|------------|
| 1 | Fundação | 1 | 100K registros | Nenhuma | Nenhuma |
| 2 | Crescimento | 3 | 1M registros | Nenhuma | Nenhuma |
| 3 | Expansão | 6+ | Múltiplos formatos | Manual | Informal |
| 4 | Maturidade | 6+ | Diário particionado | Semi-auto | Informal |
| 5 | Escala | 10+ | Multi-pipeline | Automatizada | Parcial |
| 6 | Governança | 10+ | 50+ clientes | Automatizada | Formal |
| 7 | Produção | Todas | End-to-end | Produção | Completa |
| 8 | Board Meeting | — | — | — | Auditada |

---

## Aula 1 — Fundação: O Limite do Pandas

### Situação da Empresa

A DataFlow Analytics tem 4 meses de operação. Fechou contrato com seu primeiro grande cliente de e-commerce (a **ShopBrasil**, rede com 200 lojas) e está processando dados de vendas usando scripts Python com pandas no laptop de Carlos. O time tem 6 pessoas. O clima é de entusiasmo, mas as limitações técnicas começam a aparecer: o script de relatório diário que antes levava 5 minutos agora leva 4 horas com o volume crescente de dados.

### Gatilho/Incidente

Na segunda-feira de manhã, Carlos chega ao escritório e descobre que o script de relatório de vendas **travou durante a noite** — o pandas tentou carregar 100K registros em memória e o processo foi killed pelo sistema operacional. O relatório que a ShopBrasil espera toda manhã às 8h não foi entregue. Ana recebe a ligação do cliente às 9h15.

### Desafio Técnico

Processar 100K+ registros de vendas de forma confiável, sem depender da memória de uma única máquina. Introduzir processamento distribuído com Apache Spark para substituir pandas nos workloads pesados.

### Cena de Abertura

> **Carlos** *(chegando ao escritório, olhando o laptop com cara de desespero)*: Marina, o script morreu de novo. Killed. 100 mil registros e o pandas engasgou.
>
> **Ana** *(entrando apressada, celular na mão)*: Gente, a ShopBrasil tá no telefone. O gerente comercial quer saber por que o relatório de ontem não chegou. É a terceira vez esse mês.
>
> **Marina** *(calma, levantando do café)*: Carlos, quanto tempo o script levou antes de cair?
>
> **Carlos**: Quatro horas e trinta e sete minutos. Depois: `Killed`. Sem cerimônia.
>
> **Marina** *(desenhando no quadro branco)*: Ok. Isso não é um bug — é um limite de arquitetura. Pandas roda em uma máquina, com memória finita. Precisamos de processamento distribuído. Já ouviram falar de Apache Spark?
>
> **Carlos** *(sorrindo)*: Finalmente. Eu tava esperando você falar isso desde a semana passada.

### Gancho para Próxima Aula

No final da aula, após o lab funcionar com 100K registros de forma suave no Spark, Marina recebe um e-mail: a ShopBrasil quer **expandir o contrato** — agora precisam de relatórios cruzando vendas com dados de clientes, categorias e campanhas de marketing. O volume vai ser 10x maior. Carlos olha para Marina: *"Bom, leitura simples a gente resolveu. Mas joins com 1 milhão de registros em 3 tabelas diferentes... isso é outro nível."*

### Evolução de Complexidade

- **Antes**: Scripts pandas no laptop de Carlos
- **Depois**: Cluster Spark local (Docker) processando dados em paralelo
- **Novo problema emergente**: O volume vai crescer 10x e será necessário cruzar múltiplas fontes

---

## Aula 2 — Crescimento: 10x Mais Dados, 10x Mais Complexidade

### Situação da Empresa

A DataFlow completou 8 meses de operação. O contrato expandido com a ShopBrasil trouxe receita — e complexidade. Agora são 1 milhão de registros de vendas, 500 mil registros de clientes e uma hierarquia de categorias em JSON. Ana precisa de relatórios analíticos sofisticados para a campanha de Black Friday do cliente: ranking de clientes por estado, tendências de compra, segmentação por ticket médio. O Spark já está rodando, mas Carlos percebe que operações simples não bastam.

### Gatilho/Incidente

Ana apresenta na daily meeting os requisitos da campanha de Black Friday da ShopBrasil: *"Eles precisam saber quais clientes compraram mais nos últimos 6 meses, segmentados por estado, com a variação de ticket médio mês a mês."* Carlos tenta resolver com groupBy simples e percebe que precisa de **window functions**, **joins complexos** e possivelmente **UDFs** para classificações customizadas. O prazo? Duas semanas.

### Desafio Técnico

Realizar transformações avançadas com múltiplos DataFrames: joins de diferentes tipos, window functions para rankings e análise temporal, UDFs para regras de negócio customizadas. Entender e otimizar o plano de execução do Spark.

### Cena de Abertura

> **Ana** *(projetando dashboard na TV)*: Pessoal, a ShopBrasil precisa de três relatórios pra Black Friday. Primeiro: top 100 clientes por estado. Segundo: tendência de compra mês a mês por cliente. Terceiro: segmentação automática por faixa de ticket — baixo, médio, alto.
>
> **Carlos** *(coçando a cabeça)*: Ok, o primeiro exige join de vendas com clientes e depois um ranking por partição. O segundo precisa de window function com lag temporal. O terceiro... acho que vou precisar de uma UDF.
>
> **Roberto** *(passando pela sala, café na mão)*: Duas semanas, pessoal. A Black Friday não espera. Quando isso vira relatório pro cliente?
>
> **Marina**: Carlos, mostra pra gente como o Spark resolve joins distribuídos. Se o DataFrame de categorias é pequeno, podemos fazer broadcast. Vamos pensar em escala — o que funciona hoje precisa funcionar com 10x mais dados amanhã.
>
> **Carlos** *(abrindo o terminal)*: Bora codar? Abre o notebook aí que a gente resolve isso em 10 minutos. Começo pelo join e a gente escala dali.

### Gancho para Próxima Aula

Os relatórios da Black Friday são entregues com sucesso. Roberto está satisfeito — a ShopBrasil renovou por mais um ano. Mas na comemoração, Ana deixa cair a bomba: *"Pessoal, fechamos com três novos parceiros. A PagFácil manda dados em JSON via API. O MarketLog envia CSV por SFTP, com encoding ISO-8859-1 e separador ponto-e-vírgula. E a LogiExpress tem um bucket com Parquet."* Carlos engole seco: *"Três formatos diferentes, três schemas diferentes, três cadências diferentes... vamos precisar de um processo de ingestão sério."*

### Evolução de Complexidade

- **Antes**: Uma fonte de dados (vendas CSV), operações simples
- **Depois**: Três fontes integradas (vendas + clientes + categorias), transformações avançadas com joins, windows e UDFs
- **Novo problema emergente**: Múltiplos parceiros enviam dados em formatos completamente distintos — como normalizar tudo?

---

## Aula 3 — Expansão: Dados de Múltiplos Parceiros

### Situação da Empresa

A DataFlow está com 1 ano de operação e 12 pessoas no time. Os três novos parceiros (PagFácil, MarketLog e LogiExpress) começaram a enviar dados, cada um à sua maneira. O escritório virou um caos de scripts ad-hoc: Carlos tem um script para cada parceiro, cada um com sua lógica de parsing e normalização. Não existe padrão, não existe camada organizada de dados. Marina olha para a situação e vê o que viveu no Nubank anos atrás: hora de implementar uma arquitetura de data lake com camadas.

### Gatilho/Incidente

Na quinta-feira, o script de ingestão da PagFácil falha silenciosamente porque o parceiro mudou um campo de "valor_total" para "total_venda" sem avisar. O relatório da sexta sai com zeros em 40% dos registros. Ana descobre quando o cliente liga reclamando. Marina convoca reunião emergencial: *"Não podemos ter scripts frágeis que quebram em silêncio. Precisamos de ingestão robusta com validação de schema."*

### Desafio Técnico

Implementar pipeline de ingestão batch de múltiplos formatos (CSV legado com encoding especial, JSON de API dumps, Parquet de data lake). Aplicar arquitetura medallion (Bronze/Silver/Gold), schema enforcement e particionamento inteligente.

### Cena de Abertura

> **Ana** *(na reunião emergencial, mostrando print do e-mail do cliente)*: O relatório da ShopBrasil saiu com 40% dos valores zerados. A PagFácil mudou o nome de uma coluna e ninguém percebeu. O cliente só viu na sexta de manhã.
>
> **Carlos** *(suspirando)*: Eu tenho um script separado pra cada parceiro. Eles são frágeis — qualquer mudança no schema e quebra sem aviso. Eu sei que não é o ideal, mas era o que dava pra fazer com o tempo que tínhamos.
>
> **Marina** *(no quadro branco, desenhando três caixas)*: Bronze. Silver. Gold. É assim que a gente resolve. Bronze é dado cru, como chegou. Silver é dado limpo e normalizado. Gold é dado pronto pro negócio. Se o schema mudar, a Bronze grava mesmo assim — e a Silver pega o problema na validação.
>
> **Roberto** *(no Slack, mensagem curta)*: "Marina — mais 2 parceiros assinaram ontem. Dados chegam em 2 semanas. Me diz que vocês estão prontos."
>
> **Marina** *(olhando para Carlos)*: Precisamos de um pipeline de ingestão que funcione pra qualquer parceiro. Formato diferente, schema diferente, cadência diferente — mas saída padronizada. Vamos construir isso hoje.

### Gancho para Próxima Aula

A arquitetura medallion está implementada e funcionando para os 3 parceiros. Carlos está orgulhoso — até que percebe que roda o pipeline de ingestão **manualmente** toda manhã às 6h. Com 2 novos parceiros chegando, isso significa 5 pipelines para rodar na mão todo dia. Ana olha o calendário e diz: *"Carlos, semana que vem você está de férias. Quem vai rodar os scripts às 6h da manhã?"* Silêncio. Marina sorri: *"Acho que chegou a hora de conhecerem o Apache Airflow."*

### Evolução de Complexidade

- **Antes**: Dados de uma única fonte, formato único
- **Depois**: Dados de 3+ parceiros, 3 formatos distintos, arquitetura de data lake com camadas (Bronze/Silver/Gold)
- **Novo problema emergente**: Todos os pipelines são executados manualmente — isso não escala quando o time cresce

---

## Aula 4 — Maturidade Operacional: Automação com Airflow

### Situação da Empresa

A DataFlow está com 1 ano e meio. Carlos foi de férias pela primeira vez em meses — e na segunda-feira, nenhum relatório saiu. Ninguém no time sabia rodar os 5 scripts na ordem certa, com as dependências certas. Ana teve que ligar para Carlos no meio das férias. Roberto ficou furioso: *"Não podemos depender de uma pessoa. Isso é risco operacional."* Marina sabia que esse dia chegaria — é hora de automatizar.

### Gatilho/Incidente

Carlos volta de férias e encontra 3 dias de relatórios atrasados, 2 scripts que rodaram fora de ordem (gerando dados corrompidos no Silver) e um e-mail irritado de Roberto com assunto: "Bus factor = 1. Inaceitável." Marina já tinha preparado a solução: Apache Airflow. Só esperava o momento certo para o time sentir a dor da falta de automação.

### Desafio Técnico

Transformar scripts manuais em pipelines automatizados usando Apache Airflow. Configurar DAGs com dependências entre tasks, usar PythonOperator e BashOperator, implementar comunicação entre tasks via XComs, definir schedules e retentativas automáticas.

### Cena de Abertura

> **Carlos** *(voltando de férias, olhando o monitor com pavor)*: Marina... são 47 mensagens no Slack. Três dias sem relatório. Quem rodou os scripts enquanto eu tava fora?
>
> **Marina** *(calmamente)*: Ninguém, Carlos. Esse é exatamente o ponto. A gente não pode ter um pipeline de produção que depende de um ser humano lembrar de rodar `python run_pipeline.py` todo dia às 6 da manhã.
>
> **Roberto** *(entrando na sala)*: Carlos, bom ter você de volta. Mas vamos combinar uma coisa: nunca mais a operação inteira para porque alguém saiu de férias. Isso é inadmissível em uma empresa que quer escalar.
>
> **Carlos**: Concordo. Mas o que a gente usa? Cron? Task scheduler?
>
> **Marina**: Nenhum dos dois. Cron não tem retry, não tem dependência entre tasks, não tem visibilidade. Vamos usar Apache Airflow — um orquestrador de verdade. Você define o pipeline como código Python, e ele cuida do resto: agenda, executa, retenta, notifica.
>
> **Carlos** *(aliviado)*: Quem aqui já acordou às 6h da manhã pra rodar script na mão? Nunca mais. Bora codar isso.

### Gancho para Próxima Aula

A primeira DAG está no ar: extrai, transforma, carrega, notifica. Funciona como relógio. Mas Ana traz uma nova complexidade na retrospectiva: *"O relatório consolidado só pode sair depois que TODOS os parceiros tenham sido processados. E agora temos 10 fontes. Se uma falha, o que acontece? E tem mais — na Black Friday, o volume triplica. Precisamos decidir se usa Spark ou Python puro baseado no tamanho dos dados."* Carlos olha para a DAG linear que acabou de criar e percebe: *"Vamos precisar de branching, sensores, e integração com Spark dentro do Airflow. Isso é outro nível de orquestração."*

### Evolução de Complexidade

- **Antes**: Pipelines manuais que dependem de uma pessoa específica para executar
- **Depois**: DAG automatizada no Airflow com schedule, retentativas e notificações
- **Novo problema emergente**: Pipelines lineares não resolvem dependências complexas entre 10+ fontes com condições e branching

---

## Aula 5 — Escala Corporativa: Orquestração Avançada

### Situação da Empresa

A DataFlow está com 2 anos e agora processa dados de 10 parceiros diferentes. A Black Friday está em 3 semanas e Roberto fechou um contrato enterprise com a **MegaShop** (rede de 500 lojas). O volume esperado na Black Friday é 3x o normal. As DAGs simples da Aula 4 não dão conta: algumas fontes atrasam, outras falham, e o relatório consolidado não pode sair incompleto. Marina exige orquestração robusta com sensores, branching inteligente e SLA rígido.

### Gatilho/Incidente

Na simulação de carga para a Black Friday, a DAG linear processa a fonte do MarketLog antes dos dados chegarem (o arquivo só é depositado às 7h30, mas a DAG roda às 6h). Resultado: relatório com dados de 9 parceiros mas sem o MarketLog — justo o que representa 25% do volume. Roberto recebe reclamação direta do diretor da MegaShop: *"Se isso acontecer na Black Friday, o contrato está cancelado."* São R$ 1.2M de receita em risco.

### Desafio Técnico

Implementar orquestração avançada: FileSensor para esperar dados chegarem, BranchPythonOperator para decidir fluxo baseado em volume (Spark vs Python), TaskGroups para organizar visualmente, SparkSubmitOperator para integrar Spark com Airflow, callbacks de falha para alertas automáticos.

### Cena de Abertura

> **Roberto** *(em pé na sala de reunião, tom sério)*: Vou ser direto. A MegaShop nos deu SLA: relatório pronto até 8h da manhã, todo dia, sem exceção. Na Black Friday, o volume triplica. Se falhar UMA vez, são R$ 1.2 milhão de receita que vão pro concorrente. Não me importa qual ferramenta vocês usam. Me importa que funcione.
>
> **Ana**: Na simulação de ontem, o relatório saiu sem os dados do MarketLog. O arquivo deles só chega às 7h30 e nossa DAG roda às 6h. Processamos dados incompletos.
>
> **Marina**: Precisamos de sensores — o pipeline não pode rodar até confirmar que os dados estão lá. E na Black Friday, com 3x de volume, não faz sentido processar 50GB com Python puro. O Airflow precisa decidir dinamicamente: volume grande vai pro Spark, volume pequeno vai pro Python.
>
> **Carlos**: Então estamos falando de FileSensor pra esperar os arquivos, BranchPythonOperator pra decidir o caminho, e SparkSubmitOperator pra rodar os jobs pesados. Ah, e se qualquer coisa falhar, quero alerta no Slack em 30 segundos.
>
> **Marina**: Exatamente. E vamos organizar isso com TaskGroups — 10 fontes com branching vira espaguete se não estruturar direito. Bora montar essa arquitetura.

### Gancho para Próxima Aula

A Black Friday é um sucesso. O pipeline processou 3x o volume normal, os sensores seguraram a execução até todos os dados chegarem, e o relatório da MegaShop foi entregue às 7h45 — 15 minutos antes do SLA. Roberto está eufórico. Mas na segunda-feira pós-Black Friday, Ana recebe uma ligação gelada: *"A MegaShop detectou 3 mil pedidos duplicados no relatório de sábado. O faturamento deles aparece 15% maior do que o real. O diretor financeiro quer explicações."* Marina fica séria: *"Não basta processar rápido. Precisamos processar com qualidade. Está na hora de um programa formal de data quality."*

### Evolução de Complexidade

- **Antes**: DAGs lineares simples (extrair → transformar → carregar)
- **Depois**: Orquestração avançada com sensores, branching dinâmico, integração Spark, TaskGroups e alertas
- **Novo problema emergente**: Velocidade sem qualidade é perigoso — dados duplicados e inconsistentes chegam ao cliente

---

## Aula 6 — Governança e Compliance: Qualidade de Dados

### Situação da Empresa

A DataFlow está com 2 anos e meio, processando dados de 50+ clientes. O incidente da MegaShop (3 mil pedidos duplicados) virou um alarme vermelho. Roberto convocou reunião executiva: o contrato da MegaShop tem cláusula de penalidade por dados incorretos. Além disso, a LGPD exige rastreabilidade de dados pessoais. A empresa precisa de um programa formal de qualidade de dados — não como luxo, mas como sobrevivência do negócio.

### Gatilho/Incidente

O diretor financeiro da MegaShop envia e-mail formal: *"Vocês têm 60 dias para corrigir o problema de dados duplicados. Caso contrário, exercemos a cláusula 4.2 do contrato e migramos para o DataStar."* São R$ 800K de receita anual em risco — e se a MegaShop sair, três outras contas menores vão junto. Roberto escala para nível máximo. Marina convoca Carlos e Ana: *"Precisamos de validação automática em CADA etapa do pipeline. Dados ruins não podem chegar no Gold. Nunca."*

### Desafio Técnico

Implementar framework de qualidade de dados com PySpark: checks de completude, unicidade, integridade referencial e regras de negócio. Criar sistema de quarentena para dados inválidos. Configurar monitoramento e alertas automáticos no Airflow. Implementar SLAs com notificação de violação.

### Cena de Abertura

> **Roberto** *(projetando o e-mail da MegaShop na tela)*: Sessenta dias. É o que a MegaShop nos deu. Se o problema de dados duplicados não estiver resolvido até lá, eles migram pro DataStar. E levam outras três contas junto. Estamos falando de R$ 800 mil de receita anual.
>
> **Ana** *(com printscreen da planilha)*: Gente, o relatório de sábado tinha 3 mil pedidos duplicados. Isso é 15% do faturamento deles aparecendo dobrado. O diretor financeiro está furioso — com razão.
>
> **Marina** *(séria, no quadro branco)*: O problema é claro: nosso pipeline processa rápido, mas não valida. Dado duplicado passa direto do Bronze pro Gold sem ninguém perceber. A partir de hoje, nenhum registro chega na camada Gold sem passar por validação automática.
>
> **Carlos**: O que a gente precisa? Checks de unicidade, completude, integridade referencial... e o que fazer com dado ruim? Descarta?
>
> **Marina**: Não descarta — quarentena. Dado inválido vai pra uma área separada com metadados do porquê foi rejeitado. A gente analisa depois. E se qualquer check crítico falhar, o pipeline PARA e manda alerta. Sem exceção.

### Gancho para Próxima Aula

O framework de qualidade está implementado e funcionando. A taxa de dados ruins caiu de 15% para 0.3%. A MegaShop renovó o contrato. Roberto está aliviado. Mas Marina olha para o cenário completo e vê que cada peça foi construída separadamente: ingestão aqui, transformação ali, qualidade acolá, orquestração em outro lugar. *"Pessoal, temos todas as peças do quebra-cabeça. Mas elas não estão montadas como um sistema único. Precisamos de um pipeline end-to-end — da ingestão ao relatório final — rodando em produção com Docker, com observabilidade, com idempotência. É hora de juntar tudo."* Carlos sente o peso: *"Isso é praticamente construir o sistema de produção inteiro."* Marina sorri: *"Exatamente. É o ensaio geral antes do board meeting."*

### Evolução de Complexidade

- **Antes**: Pipelines processam rápido mas sem validação — dados ruins chegam ao cliente
- **Depois**: Framework de qualidade com checks automáticos, quarentena, monitoramento e SLA com alertas
- **Novo problema emergente**: Todas as peças existem isoladamente — precisam ser integradas em um pipeline end-to-end de produção

---

## Aula 7 — Integração Total: Pipeline End-to-End em Produção

### Situação da Empresa

A DataFlow está prestes a completar 3 anos. Roberto marcou reunião de board para daqui a um mês — investidores querem ver a plataforma funcionando end-to-end. Marina sabe que todas as peças existem (Spark, Airflow, qualidade, ingestão multi-formato), mas estão espalhadas em notebooks, scripts soltos e DAGs de teste. É hora de construir o **pipeline de produção real**: containerizado, observável, idempotente e documentado. Carlos vai liderar a integração técnica — é seu maior desafio na DataFlow até agora.

### Gatilho/Incidente

Roberto confirma a data do board meeting e adiciona: *"Os investidores querem demo ao vivo. Não slide bonito — pipeline rodando de verdade, processando dados de verdade, com métricas na tela."* Marina faz o checklist mental: ingestão multi-parceiro → transformações com Spark → orquestração com Airflow → qualidade em cada camada → tudo em Docker → logs e métricas visíveis. Carlos olha o checklist e diz: *"Temos 4 semanas. Vou precisar de café."*

### Desafio Técnico

Integrar Spark + Airflow em pipeline completo containerizado com Docker Compose. Implementar jobs Spark de produção com logging estruturado, idempotência (overwrite por partição) e tratamento de erros. Criar DAG Airflow que orquestra o fluxo Bronze → Silver → Gold com checks de qualidade em cada transição.

### Cena de Abertura

> **Roberto** *(em videoconferência com o time)*: Board meeting em 4 semanas. Os investidores querem ver o pipeline rodando — ao vivo. Não quero slide com diagrama bonito. Quero ver dado entrando de um lado e relatório saindo do outro. Com números reais.
>
> **Marina** *(anotando)*: Então precisamos do pipeline completo em produção: ingestão, transformação, qualidade e entrega. Tudo containerizado, tudo automatizado, tudo com métricas.
>
> **Carlos**: Marina, a gente tem cada peça separada. Ingestão funciona. Transformações funcionam. Airflow funciona. Qualidade funciona. O que falta é grudar tudo num sistema coeso. É como ter todas as peças do LEGO fora da caixa — agora precisa montar.
>
> **Ana** *(complementando)*: E precisa ter critérios claros de sucesso. Pro board, eu definiria: SLA de entrega até 8h, completude acima de 99.5%, zero duplicatas na camada Gold, e log de cada execução rastreável.
>
> **Marina**: Perfeito. Carlos, você lidera. Docker Compose com Spark, Airflow e monitoramento. Job Spark de produção com logging estruturado. DAG que orquestra Bronze-Silver-Gold. Idempotência em cada etapa — se precisar reprocessar, não pode gerar duplicata. Esse é o ensaio geral.
>
> **Carlos** *(sorrindo, determinado)*: Bora montar esse LEGO. Começo pelo Docker Compose e subo camada por camada.

### Gancho para Próxima Aula

O pipeline end-to-end está rodando em produção. Dados entram automaticamente de 5 parceiros, passam pelo Bronze/Silver/Gold com checks de qualidade, e o relatório é entregue todo dia às 7h30 — antes do SLA de 8h. Marina valida a arquitetura. Carlos está orgulhoso. Roberto envia mensagem no grupo: *"Perfeito. Agora quero que CADA EQUIPE monte algo assim para um vertical diferente. O board meeting é em 2 semanas. Cada grupo vai apresentar sua solução — como se estivesse apresentando para investidores de verdade. Preparem-se."* É o início do projeto final.

### Evolução de Complexidade

- **Antes**: Peças isoladas (Spark, Airflow, qualidade, ingestão) funcionando separadamente
- **Depois**: Pipeline end-to-end integrado em produção, containerizado, com observabilidade e idempotência
- **Novo problema emergente**: Cada equipe precisa replicar essa arquitetura para um cenário novo e apresentar ao board

---

## Aula 8 — Board Meeting: Apresentação do Projeto Final

### Situação da Empresa

A DataFlow completa 3 anos. A empresa cresceu de 6 para 40 pessoas, processa dados de 50+ clientes, e está prestes a levantar sua Série A. O board meeting de hoje é o momento de demonstrar que a equipe de dados é capaz de resolver problemas complexos de diferentes verticais. Cada equipe (squad) recebeu um cenário de cliente de um setor diferente (saúde, finanças, logística, etc.) e precisa apresentar um pipeline funcional — como se estivesse fazendo pitch para investidores.

### Gatilho/Incidente

Roberto abre o board meeting com uma frase: *"Hoje não é dia de promessa. É dia de demonstração. Cada equipe tem 20 minutos para me convencer de que consegue resolver Big Data em produção para um vertical novo. Pipeline rodando. Dados fluindo. Métricas na tela. Quem não entregar, não está pronto para a Série A."* A pressão é real, mas a preparação de 7 aulas deu a cada equipe todas as ferramentas necessárias.

### Desafio Técnico

Cada grupo deve demonstrar aplicação integrada de TODAS as tecnologias do curso: pipeline PySpark com transformações relevantes, DAG Airflow com 4+ tasks orquestradas, ambiente Docker funcional, checks de qualidade de dados, arquitetura medallion e documentação clara. A demo ao vivo é obrigatória — não basta mostrar slides.

### Cena de Abertura

> **Roberto** *(de pé na frente da sala, tela grande atrás)*: Bom dia a todos. Vocês passaram 7 semanas construindo competência em Big Data. Spark. Airflow. Docker. Qualidade. Pipeline end-to-end. Hoje é o dia de mostrar que aprenderam de verdade.
>
> **Marina** *(ao lado de Roberto)*: Cada grupo recebeu um cenário de negócio de um vertical diferente. Saúde, finanças, logística, varejo. O desafio foi montar um pipeline completo — da ingestão ao relatório — usando tudo que aprendemos. Eu vou avaliar a arquitetura técnica.
>
> **Ana**: E eu vou avaliar se a solução resolve o problema de negócio. Não adianta ter código bonito se o cliente não recebe o que precisa.
>
> **Roberto**: Vinte minutos por grupo. Demo ao vivo obrigatória. Resultado. Me mostra o resultado. A tecnologia é meio, não fim. Primeiro grupo, podem começar.

### Gancho para Próxima Aula

*(Não há próxima aula — este é o encerramento do curso)*

No encerramento, após todas as apresentações e feedback, Roberto quebra a quarta parede narrativa e fala diretamente aos alunos:

> **Roberto** *(sorrindo pela primeira vez no curso)*: Vocês acabaram de fazer o que equipes de dados em empresas reais fazem todo dia: resolver problemas complexos, trabalhar sob pressão, entregar resultado com qualidade e defender decisões técnicas para stakeholders. A DataFlow é ficção — mas o que vocês construíram aqui é real. Levem isso pro próximo desafio.

### Evolução de Complexidade

- **Antes**: Aprendizado guiado com exercícios step-by-step em cenário único (DataFlow)
- **Depois**: Aplicação autônoma em cenário novo, demonstrando domínio completo do stack
- **Resolução do arco**: A DataFlow (e os alunos) provaram que dominam Big Data em produção

---

## Resumo dos Ganchos entre Aulas

| Transição | Gancho | Emoção |
|-----------|--------|--------|
| 1 → 2 | ShopBrasil expande contrato — 10x mais dados com múltiplas fontes | Empolgação + desafio |
| 2 → 3 | 3 novos parceiros com formatos completamente diferentes de dados | Urgência + escala |
| 3 → 4 | Carlos vai de férias e ninguém sabe rodar os scripts — relatórios param | Frustração + necessidade |
| 4 → 5 | Black Friday com 10 fontes, dependências complexas e SLA rigoroso | Pressão + stakes altos |
| 5 → 6 | MegaShop detecta 3K duplicatas — ameaça cancelar contrato de R$ 800K | Crise + consequência real |
| 6 → 7 | Board meeting marcado — investidores querem demo ao vivo do pipeline completo | Deadline + integração |
| 7 → 8 | Cada equipe deve replicar a arquitetura para um vertical novo e apresentar | Autonomia + prova |

---

## Princípios Narrativos

### 1. Complexidade Monotônica Crescente
Cada aula apresenta um problema MAIS complexo que o anterior. Nunca há regressão de dificuldade. O que foi aprendido nas aulas anteriores é pré-requisito para a atual.

### 2. Consequências Reais
Os incidentes têm consequência de negócio: clientes reclamam, receita está em risco, prazos são reais. Isso cria urgência emocional que motiva o aprendizado técnico.

### 3. Cada Personagem em Seu Papel
- **Marina** apresenta o problema e a direção estratégica
- **Carlos** guia a solução prática
- **Ana** traz a perspectiva do cliente/negócio
- **Roberto** cria pressão por prazos e resultados

### 4. Cliffhangers Naturais
Cada gancho emerge organicamente da resolução do problema da aula atual: resolver um problema revela o próximo. Não são artificiais — são consequências lógicas do crescimento da empresa.

### 5. Espelhamento com o Aluno
A jornada da DataFlow espelha a jornada do aluno: começa com algo simples (pandas → Spark) e termina com domínio completo do stack (pipeline end-to-end). O aluno "cresce" junto com a empresa.

---

## Guia de Uso

### Nos Slides (10 min de abertura narrativa)
- Usar 2-3 slides com a situação da empresa e a cena de abertura
- Incluir o diálogo entre personagens (pode ser em formato de quote/citação)
- Terminar com o desafio técnico que motiva a teoria

### Nos Labs (contexto narrativo)
- Referenciar a situação da empresa no início do lab
- Conectar cada exercício com uma demanda de negócio da DataFlow
- Exemplo: "A ShopBrasil precisa do relatório de Black Friday. Implemente..."

### No Encerramento de Cada Aula (gancho)
- Dedicar os últimos 2-3 minutos ao gancho narrativo
- Criar expectativa para a próxima aula sem spoiler técnico
- Formato: "Na próxima aula, vamos resolver [situação] usando [tecnologia]"

---

*Este documento deve ser consultado ao criar os slides e labs de cada aula, garantindo que a narrativa seja consistente e os ganchos conectem as aulas de forma natural.*
