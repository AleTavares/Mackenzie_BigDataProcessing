# Desafio — Aula 5: DAG Avançada com Orquestração Completa

## Objetivo

Criar uma DAG que integre **todos os padrões** vistos nas 3 DAGs de exemplo desta aula:
FileSensor + BranchPythonOperator + TaskGroups + SparkSubmitOperator + Callbacks.

---

## Requisitos

1. **10+ tasks** organizadas em TaskGroups
2. **FileSensor** aguardando arquivo de vendas do dia (path com template `{{ ds_nodash }}`)
3. **BranchPythonOperator** que decide a rota com base no dia da semana:
   - Segunda a quinta → processamento normal
   - Sexta → processamento completo (inclui relatório semanal)
4. **2 TaskGroups:**
   - `ingestao`: sensor + verificação de volume
   - `processamento`: Spark job + quality check
5. **Callback de falha** (`on_failure_callback`) que loga informações da task que falhou
6. **trigger_rule** na task de convergência (`none_failed_min_one_success`)
7. **SparkSubmitOperator** (ou PythonOperator simulando) para o job principal
8. **Templates Jinja** em pelo menos 3 tasks diferentes (`{{ ds }}`, `{{ ds_nodash }}`, `{{ prev_ds }}`)

---

## Grafo Esperado

```
                                    ┌─── TaskGroup: proc_normal ─────────┐
                                    │  spark_job ──► quality_check       │
                              ┌────►│                                    │
                              │     └────────────────────────────────────┘
┌──────────┐  ┌──────────┐   │                                              ┌───────────┐  ┌──────────┐
│  sensor  │─►│ decidir  │───┤                                         ────►│ convergir │─►│ notificar│
│ (File)   │  │ (Branch) │   │                                              │(trigger)  │  │  final   │
└──────────┘  └──────────┘   │                                              └───────────┘  └──────────┘
                              │     ┌─── TaskGroup: proc_completo ──────┐
                              │     │  spark_job ──► quality ──► report │
                              └────►│                                    │
                                    └────────────────────────────────────┘
```

---

## Dicas

- Use `datetime.strptime(context["ds"], "%Y-%m-%d").weekday()` para obter o dia da semana (0=seg, 4=sex)
- O BranchPythonOperator deve retornar o task_id completo com o nome do TaskGroup: `"proc_normal.spark_job_normal"`
- FileSensor com `mode="poke"` e `timeout=120` para não travar muito no lab
- Para simular SparkSubmitOperator sem Spark real, use PythonOperator com `print("spark-submit ...")`

---

## Bonus

- Adicione um **TimeDeltaSensor** que espera 5 segundos antes do processamento
- Implemente 3 caminhos no branching (dia normal, sexta, fim de semana)
- Use `pool="spark_pool"` para limitar paralelismo
- Configure SLA de 30 minutos nas tasks críticas

---

## Como Testar

1. Salve como `dag_desafio_avancada_<seu_nome>.py` na pasta `dags/`
2. Acesse Airflow UI: http://localhost:8081
3. Verifique que a DAG aparece sem erros (sem ícone ⚠️)
4. Execute trigger manual
5. Observe: sensor pode dar timeout (normal se arquivo não existe) — o branching e convergência são o foco

**Boa sorte!** Esta DAG é muito similar ao que vocês precisarão no Projeto Final.
