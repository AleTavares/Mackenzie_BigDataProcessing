# 📋 Release Notes — Big Data Processing

| Campo        | Valor                                      |
| ------------ | ------------------------------------------ |
| **Curso**    | Big Data Processing                        |
| **Versão**   | v1.0                                       |
| **Data**     | Junho 2024                                 |
| **Instituição** | Universidade Presbiteriana Mackenzie     |

---

## Visão Geral

Este release representa a versão inicial completa do ambiente de laboratório do curso **Big Data Processing**. O pacote inclui 8 aulas estruturadas com labs práticos, cobrindo desde fundamentos de Apache Spark até orquestração de pipelines com Apache Airflow. Todo o ambiente é containerizado via Docker Compose, garantindo reprodutibilidade entre máquinas dos alunos.

---

## Versões de Tecnologias

### Tecnologias Core

| Tecnologia       | Versão | Observação                          |
| ---------------- | ------ | ----------------------------------- |
| Apache Spark     | 3.5    | Engine de processamento distribuído |
| Apache Airflow   | 2.8.4  | Orquestrador de pipelines          |
| Python           | 3.11   | Base da imagem Jupyter              |
| Docker Compose   | v2     | Orquestração de containers         |

### Bibliotecas Python

| Biblioteca       | Versão  | Finalidade                          |
| ---------------- | ------- | ----------------------------------- |
| pyspark          | 3.5.3   | Interface Python para Spark         |
| apache-airflow   | 2.8.4   | Orquestração de workflows          |
| pandas           | 2.2.1   | Manipulação de dados tabulares      |
| numpy            | 1.26.4  | Computação numérica                 |
| matplotlib       | 3.8.3   | Visualização de dados               |
| seaborn          | 0.13.2  | Visualização estatística            |
| pytest           | 8.1.1   | Framework de testes                 |
| faker            | 24.3.0  | Geração de dados sintéticos         |
| pyarrow          | 15.0.2  | Suporte a formatos colunares        |

---

## Imagens Docker

| Imagem                        | Tag      | Uso no Curso                          |
| ----------------------------- | -------- | ------------------------------------- |
| bitnami/spark                 | 3.5      | Spark Master e Worker                 |
| apache/airflow                | 2.8      | Airflow Webserver e Scheduler         |
| jupyter/pyspark-notebook      | latest   | Ambiente Jupyter para notebooks       |

---

## Requisitos de Sistema

### Hardware Mínimo

| Recurso           | Mínimo Recomendado |
| ----------------- | ------------------ |
| Memória RAM       | 8 GB               |
| Processador       | 4 cores            |
| Espaço em disco   | 20 GB livres       |

### Software Necessário

- Docker Desktop (Windows/macOS) ou Docker Engine (Linux)
- Docker Compose v2
- Git
- VS Code (recomendado)

### Extensões VS Code Recomendadas

| Extensão                          | ID                            |
| --------------------------------- | ----------------------------- |
| Python                            | ms-python.python              |
| Jupyter                           | ms-toolsai.jupyter            |
| Docker                            | ms-azuretools.vscode-docker   |
| YAML                              | redhat.vscode-yaml            |

---

## Compatibilidade

### Sistemas Operacionais Testados

| Sistema Operacional         | Status      | Observação                        |
| --------------------------- | ----------- | --------------------------------- |
| Ubuntu 22.04 (nativo/WSL2) | ✅ Testado  | Ambiente principal de desenvolvimento |
| macOS 13+ (Ventura)        | ✅ Testado  | Requer Docker Desktop             |
| Windows 11 com WSL2        | ✅ Testado  | Requer Docker Desktop + WSL2      |

### Navegadores para Interfaces Web

- Google Chrome 120+ (recomendado)
- Mozilla Firefox 120+
- Microsoft Edge 120+

---

## Notas de Compatibilidade

> ⚠️ **Atenção:** Leia antes de atualizar qualquer dependência.

- **PySpark 3.5.x → 4.0**: A versão 4.0 do Spark introduzirá breaking changes na API de DataFrames. Não atualize sem validar os notebooks.
- **Airflow 2.8.x → 2.9.x**: Mudanças na API de serialização de DAGs podem exigir ajustes nos exemplos.
- **Pandas 2.x**: A versão 2.x deprecou `append()` e alterou comportamentos de `inplace`. Os notebooks já utilizam a API atualizada.
- **NumPy 2.0**: Caso atualizado, pode quebrar compatibilidade com versões anteriores de pandas e matplotlib. Manter em 1.26.x.
- **PyArrow 15.x**: Necessário para suporte a Parquet nos labs de persistência. Versões anteriores a 12.0 não são compatíveis.

---

## Changelog

### v1.0 — Release Inicial (Junho 2024)

**Conteúdo incluído:**

- ✅ 8 aulas completas com slides, labs e código de exemplo
- ✅ Ambiente Docker Compose com Spark, Airflow e Jupyter
- ✅ Labs progressivos: do básico ao pipeline completo
- ✅ Dados sintéticos gerados via Faker para todos os exercícios
- ✅ Documentação de troubleshooting por aula
- ✅ Entregáveis definidos para cada lab
- ✅ Suporte a execução local e GitHub Codespaces

**Aulas incluídas:**

| Aula   | Tema                                        |
| ------ | ------------------------------------------- |
| Aula 01 | Fundamentos de Spark e DataFrames          |
| Aula 02 | Spark SQL, UDFs e Planos de Execução       |
| Aula 03 | Ingestão de Dados e Arquitetura Medalhão   |
| Aula 04 | Orquestração com Apache Airflow            |
| Aula 05 | Pipelines Spark + Airflow integrados       |
| Aula 06 | Testes e Qualidade de Dados                |
| Aula 07 | Otimização e Performance                   |
| Aula 08 | Projeto Final                              |

---

## Limitações Conhecidas

| # | Limitação                                                    | Workaround                                      |
| - | ------------------------------------------------------------ | ----------------------------------------------- |
| 1 | Spark UI pode demorar para iniciar no primeiro boot          | Aguardar ~30s após `docker compose up`          |
| 2 | Airflow scheduler precisa de ~60s para detectar novas DAGs   | Usar `airflow dags reserialize` para forçar     |
| 3 | GitHub Codespaces requer machine type 4-core mínimo          | Selecionar "4-core" na criação do Codespace     |
| 4 | Primeiro `docker compose up` baixa ~5GB de imagens           | Executar com antecedência em rede estável        |
| 5 | Hot-reload de notebooks pode falhar após restart do Spark    | Reiniciar kernel do Jupyter                     |

---

## Roadmap — Próximas Versões

### v1.1 (Planejado)

- 🔄 Atualização para Apache Spark 4.0 quando versão estável for lançada
- 🔄 Migração do Apache Airflow para 2.9.x
- 🔄 Adição de Delta Lake nos labs de persistência (Aula 03)

### v1.2 (Futuro)

- 📦 Integração com Apache Iceberg como alternativa ao Delta Lake
- 📦 Labs de streaming com Spark Structured Streaming
- 📦 Monitoramento com Prometheus + Grafana

---

## Suporte

Para dúvidas ou problemas com o ambiente:

1. Consulte o arquivo `troubleshooting.md` da aula correspondente
2. Verifique se sua versão do Docker está atualizada
3. Abra uma issue no repositório do curso

---

*Documento gerado para o curso Big Data Processing — Universidade Presbiteriana Mackenzie*
