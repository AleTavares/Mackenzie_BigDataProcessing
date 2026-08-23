# Aula 6 — Qualidade de Dados e Monitoramento

**Disciplina:** Big Data Processing — MBA Engenharia de Dados (Mackenzie)

---

## Contexto Narrativo

> **"Dados ruins, decisões ruins."**
>
> Um bug silencioso: 15% dos registros de um parceiro chegam com valores negativos e campos nulos. O relatório foi para o board com números errados. Marina decreta: nenhum dado entra em produção sem validação. Nasce o framework de qualidade da DataFlow.

---

## Objetivos de Aprendizagem

Ao final desta aula, o aluno será capaz de:

1. **Identificar** as dimensões de qualidade de dados (completude, unicidade, integridade, validade)
2. **Implementar** checks de qualidade com PySpark
3. **Construir** um sistema de quarentena para dados inválidos
4. **Criar** relatório consolidado de qualidade
5. **Integrar** validações em DAGs do Airflow (quality gates)
6. **Decidir** quando bloquear ou alertar sobre problemas de qualidade

---

## Estrutura da Aula (4 horas)

| Bloco | Conteúdo | Duração |
|-------|----------|---------|
| Teoria | Slides HTML — Dimensões de qualidade, checks, quarentena | 50 min |
| Intervalo | — | 10 min |
| Lab Parte 1 | Checks de completude + unicidade + integridade (guiado) | 60 min |
| Intervalo | — | 10 min |
| Lab Parte 2 | Quarentena + Framework completo + Desafio | 50 min |

---

## Estrutura de Arquivos

```
aula_06/
├── README.md                  # Este arquivo
├── aula_06_slides.html        # Slides da teoria (HTML interativo)
├── code/
│   └── aula06_lab.ipynb       # Notebook do laboratório
├── data/
│   └── .gitkeep
└── lab/
    ├── README.md              # Visão geral do lab
    ├── 01_check_completude.md # Exercício: nulls e campos obrigatórios
    ├── 02_check_unicidade.md  # Exercício: deduplicação e chaves únicas
    ├── 03_check_integridade.md # Exercício: integridade referencial
    ├── 04_quarentena.md       # Exercício: sistema de quarentena
    ├── 05_dag_qualidade.md    # Exercício: DAG com quality gates
    ├── 06_framework_qualidade.md # Desafio: framework reutilizável
    ├── 07_troubleshooting.md  # Guia de problemas
    └── ENTREGAVEL.md
```

---

## Tópicos Abordados

- 6 Dimensões de qualidade: completude, unicidade, validade, integridade, consistência, atualidade
- Check de completude (null count, campos obrigatórios)
- Check de unicidade (duplicatas por chave primária)
- Check de integridade referencial (foreign keys)
- Check de validade (ranges, domínios, formatos)
- Sistema de quarentena (separar dados inválidos)
- Relatório de qualidade consolidado (métricas por check)
- Quality Gate: bloquear pipeline se qualidade < threshold
- Integração com Airflow (alertas, callbacks)

---

## Rodar no Google Colab (recomendado)

Se não quiser configurar Docker local, use o Google Colab:

1. Acesse [colab.research.google.com](https://colab.research.google.com)
2. Menu **Arquivo → Abrir notebook → GitHub**
3. Cole a URL do repositório: `https://github.com/AleTavares/Mackenzie_BigDataProcessing`
4. Selecione o notebook `aula_06/code/aula06_lab.ipynb`
5. Adicione esta célula no topo antes de rodar:

```python
# === SETUP COLAB ===
!pip install pyspark -q
!git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git /content/repo 2>/dev/null
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Path dos datasets (ajuste para Colab)
DATA_PATH = "/content/repo/datasets/aula_06"
```

> **Nota:** No Colab, substitua os paths `/home/jovyan/work/data/aula_06/` por `/content/repo/datasets/aula_06/` nas células de leitura.

---

## Pré-requisitos

- Ter completado as Aulas 1-5 (Spark + Airflow)
- Docker rodando com pelo menos 8 GB de RAM
- Stack completa: `docker compose -f shared/docker-compose.full.yml up -d`

---

## Navegação

⬅️ [Aula 5 — Orquestração Avançada](../aula_05/) · ➡️ [Aula 7 — Pipeline End-to-End](../aula_07/)
