# Desafio — Aula 4: Crie Sua Própria DAG

## Objetivo

Criar uma DAG que simule o pipeline de processamento de **múltiplos parceiros** da DataFlow Analytics.

---

## Requisitos

1. **DAG com 6+ tasks** incluindo padrão fan-out (extração paralela de 2 parceiros)
2. **PythonOperator** em pelo menos 3 tasks
3. **BashOperator** em pelo menos 1 task com template `{{ ds }}`
4. **XCom** para passar metadados entre tasks (ex: total de registros extraídos)
5. **Retries** configurado com pelo menos 2 tentativas
6. **Dependências fan-out/fan-in:**
   ```
   extrair_parceiro_a ──┐
                        ├──► consolidar ──► notificar
   extrair_parceiro_b ──┘
   ```

---

## Estrutura Sugerida

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Funcoes para cada parceiro
def extrair_parceiro_a(**context):
    # Simula extracao do parceiro A (CSV legado)
    # Return envia XCom automaticamente
    return {"parceiro": "A", "registros": 800, "formato": "csv"}

def extrair_parceiro_b(**context):
    # Simula extracao do parceiro B (API JSON)
    return {"parceiro": "B", "registros": 700, "formato": "json"}

def consolidar(**context):
    # Puxa XCom de ambos parceiros
    ti = context["ti"]
    info_a = ti.xcom_pull(task_ids="extrair_parceiro_a")
    info_b = ti.xcom_pull(task_ids="extrair_parceiro_b")
    total = info_a["registros"] + info_b["registros"]
    print(f"Total consolidado: {total}")
    return total

# ... monte a DAG com as dependencias
```

---

## Bonus

- Adicione `op_kwargs` para parametrizar as funções (nome do parceiro, caminho do arquivo)
- Use `{{ ds_nodash }}` além de `{{ ds }}` em templates
- Adicione `tags` descritivos na DAG
- Implemente `trigger_rule="all_done"` na task de notificação

---

## Como Testar

1. Salve o arquivo como `dag_desafio_<seu_nome>.py` na pasta `dags/`
2. Acesse Airflow UI: http://localhost:8081
3. Verifique que a DAG aparece sem erros
4. Execute um trigger manual
5. Confira que todas as tasks ficam verdes

**Boa sorte!**
