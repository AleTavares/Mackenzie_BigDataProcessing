# Aula 8 — Projeto Final

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Reunião do conselho."**
>
> A DataFlow vai apresentar seus projetos ao board de investidores. Cada equipe de engenharia demonstra seu pipeline ao vivo. Não basta slides bonitos — os investidores querem ver o produto funcionando: `docker compose up` → pipeline rodando end-to-end.

---

## Objetivo

Esta é a aula de **apresentação dos projetos finais**. Não há conteúdo teórico novo. Os grupos demonstram seus pipelines de dados completos com demo ao vivo.

---

## Formato da Apresentação

| Item | Duração |
|------|---------|
| Apresentação + Demo ao vivo | 20 min |
| Perguntas (professor + colegas) | 5 min |
| **Total por grupo** | **25 min** |

### Regras

- **Todos os integrantes** devem participar (falar pelo menos 2 min cada)
- **Demo ao vivo obrigatória** — `docker compose up` → pipeline funcionando
- Tenha um **plano B** (screenshots/vídeo) caso Docker falhe no dia
- Cronômetro visível — aos 20 min a apresentação é encerrada

---

## Avaliação

| Critério | Peso |
|----------|------|
| Pipeline funciona (`docker compose up` → Bronze → Silver → Gold) | 30% |
| Arquitetura Medallion (separação clara de camadas) | 20% |
| Qualidade de dados (3+ checks, quarentena funcional) | 20% |
| Orquestração Airflow (DAG funcional, 4+ tasks) | 15% |
| Documentação (README, diagrama, instruções) | 15% |

---

## Estrutura de Arquivos

```
aula_08/
├── README.md              # Este arquivo
└── PROJETO_FINAL.md       # Especificação completa do projeto final
```

---

## Prazos

| Item | Prazo |
|------|-------|
| Formação dos grupos (3-5 pessoas) | Até o final da Aula 5 |
| Repositório pronto | 48h antes da Aula 8 |
| Submissão via Form | Durante a Aula 8 (link no dia) |
| Apresentação | Durante a Aula 8 |

---

## Especificação Completa

📋 Todos os detalhes (opções de projeto, requisitos técnicos, stack obrigatória, estrutura do repositório, checklist de entrega) estão em:

➡️ **[PROJETO_FINAL.md](./PROJETO_FINAL.md)**

---

## Stack Obrigatória

| Tecnologia | Versão |
|-----------|--------|
| Python | 3.10+ |
| Apache Spark (PySpark) | 3.5.x |
| Apache Airflow | 2.8.x |
| Docker Compose | 2.x |
| Formato de saída | Parquet |

---

## Navegação

⬅️ [Voltar ao início do curso](../)
