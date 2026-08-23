# Lab Aula 6 — Qualidade de Dados e Monitoramento

## Visão Geral

Laboratório hands-on da Aula 6, onde os alunos implementam um framework de qualidade de dados com PySpark. O cenário simula a detecção de dados corrompidos de parceiros da DataFlow e a construção de um sistema de quarentena com quality gates.

### Objetivos de Aprendizagem

- Implementar checks de completude (campos obrigatórios, null count)
- Implementar checks de unicidade (duplicatas por chave primária)
- Implementar checks de integridade referencial (foreign keys)
- Construir sistema de quarentena para dados inválidos
- Criar quality gates integrados ao Airflow
- Gerar relatório consolidado de qualidade

---

## Orçamento de Tempo (Time Budget)

| # | Exercício | Arquivo | Duração |
|---|-----------|---------|---------|
| — | **Lab Parte 1 — Guiado (60 min)** | | |
| 1 | Check de completude | `01_check_completude.md` | 20 min |
| 2 | Check de unicidade | `02_check_unicidade.md` | 20 min |
| 3 | Check de integridade referencial | `03_check_integridade.md` | 20 min |
| | **Subtotal Parte 1** | | **60 min** |
| — | **Lab Parte 2 — Intermediário + Desafio (50 min)** | | |
| 4 | Sistema de quarentena | `04_quarentena.md` | 15 min |
| 5 | DAG com quality gates | `05_dag_qualidade.md` | 15 min |
| 6 | Desafio: framework reutilizável | `06_framework_qualidade.md` | 20 min |
| | **Subtotal Parte 2** | | **50 min** |
| | | | |
| | **Total Hands-on** | | **110 min** |

---

## Índice de Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `01_check_completude.md` | Exercício guiado: verificar campos obrigatórios e null counts |
| `02_check_unicidade.md` | Exercício guiado: detectar e tratar duplicatas |
| `03_check_integridade.md` | Exercício guiado: validar foreign keys entre tabelas |
| `04_quarentena.md` | Exercício intermediário: separar dados inválidos |
| `05_dag_qualidade.md` | Exercício intermediário: integrar validações em DAG Airflow |
| `06_framework_qualidade.md` | Desafio: framework genérico e reutilizável |
| `07_troubleshooting.md` | Guia de resolução de problemas comuns |
| `ENTREGAVEL.md` | Especificação do entregável da aula |

---

## Notas para o Instrutor

- Os datasets desta aula contêm **erros intencionais** (nulls, duplicatas, valores negativos) para os alunos detectarem.
- No **Exercício 3**, a integridade referencial exige que alunos já dominem anti-joins (Aula 2).
- O **Exercício 5** integra Airflow — garanta que o ambiente completo está rodando.
- O **Desafio** pede um framework genérico — incentive os alunos a pensar em reutilização.
