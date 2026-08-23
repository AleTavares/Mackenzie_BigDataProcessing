# Desafio — Aula 7: Adapte o Pipeline para Outra Vertical

## Objetivo

Adaptar o pipeline end-to-end desta aula (Spark + Airflow + Docker) para uma **vertical diferente** de e-commerce. Isso simula o que vocês farão no Projeto Final.

---

## Requisitos

1. **Escolha uma vertical** (pode ser a mesma que seu grupo usará no Projeto Final):
   - Saúde (internações, procedimentos)
   - Finanças (transações, pagamentos)
   - Logística (entregas, rotas)
   - Educação (matrículas, notas)

2. **Adapte o `pipeline_vendas.py`** para processar dados da sua vertical:
   - Renomeie para `pipeline_<vertical>.py`
   - Ajuste a leitura de dados (schema diferente)
   - Implemente pelo menos 2 transformações Silver relevantes
   - Crie 1 agregação Gold que faça sentido para o negócio

3. **Adapte a DAG** para orquestrar o novo pipeline:
   - FileSensor aguardando o arquivo correto
   - SparkSubmitOperator chamando o novo script
   - Quality checks validando regras do novo domínio

4. **Mantenha a idempotência**: re-executar não deve duplicar dados

5. **Mantenha logging estruturado**: logs com timestamp, level e contexto

---

## Estrutura Esperada

```
meu_pipeline/
├── pipeline_<vertical>.py      # Script Spark com argparse + logging
├── dag_<vertical>.py           # DAG Airflow com sensor + spark + QA
└── README.md                   # Como rodar (1 parágrafo)
```

---

## Dicas

- Use o `pipeline_vendas.py` como template (copie e adapte)
- Crie um CSV simples com 100-1000 registros da sua vertical como dado de entrada
- O quality check pode validar regras simples: campos obrigatórios, valores em range
- Se não tiver dados reais, invente! O importante é a estrutura do pipeline
- Este desafio é o melhor preparo para o Projeto Final

---

## Bonus

- Adicione um segundo Spark job no pipeline (ex: gerar métricas de SLA)
- Implemente `partitionOverwriteMode=dynamic` para idempotência real
- Adicione testes unitários para funções de transformação (pytest)
- Crie um script de seed que gera dados fake para o sensor detectar

---

## Validação

```bash
# 1. Script Spark funciona isoladamente
python pipeline_<vertical>.py --data-ref 2024-01-15

# 2. DAG parseia sem erros
python dag_<vertical>.py

# 3. (Se Docker subir) trigger manual executa com sucesso
```

**Boa sorte!** Este é o último lab antes do Projeto Final. Use-o como prova de conceito.
