---

# PTA – Engenharia de Dados

### API de Tratamento, Validação e Padronização de Datasets para Pipeline Automatizado

Este projeto implementa uma API em **FastAPI** responsável por receber quatro datasets brutos provenientes de uma automação no **n8n**, realizar limpeza, padronização, validação cruzada, enriquecimento de dados e persistência em uma camada *Stage* (mock).
Após o processamento, a API retorna os datasets já tratados, prontos para uso analítico.

O objetivo é fornecer uma solução completa de **ETL automatizado**, garantindo consistência entre tabelas, correção de valores inconsistentes e derivação de indicadores úteis para análise e tomada de decisão.

---

# Arquitetura Geral

```
app/
 ├── main.py
 ├── routers/
 │    └── data_processing.py
 ├── schemas/
 │    └── data_schemas.py
 ├── services/
 │    ├── clean_vendedores.py
 │    ├── clean_produtos.py
 │    ├── clean_itens.py
 │    ├── clean_pedidos.py
 │    └── data_saver.py
data/
 ├── stage_vendedores.csv
 ├── stage_produtos.csv
 ├── stage_itens_pedidos.csv
 └── stage_pedidos.csv
```

A arquitetura segue um padrão modular:

* **routers/** → definição de endpoints
* **schemas/** → modelos de entrada e saída
* **services/** → regras de limpeza, validação e persistência
* **data/** → camada *stage* (mock) para armazenar CSVs tratados

---

# Componentes e Funcionalidades

## 1. Router (`data_processing.py`)

### **POST `/process-dataset`**

Executa o pipeline completo:

* recebe os 4 datasets brutos
* converte para DataFrames
* executa limpeza e validação em paralelo
* persiste os datasets tratados
* retorna um envelope com todos os dados enriquecidos

Executado com processamento concorrente via:

```python
asyncio.gather(...)
```

### **GET `/`**

Retorna mensagem de boas-vindas.

### **GET `/health`**

Verifica a saúde da API.

---

## 2. Schemas (`data_schemas.py`)

### **Schemas de Entrada (brutos)**

Representados por:

* `VendedorSchema`
* `ProdutoSchema`
* `ItemPedidoSchema`
* `PedidoSchema`

Estrutura esperada pelo endpoint:

```python
class SchemaRecepcaoDatasets:
    dataset1_vendedores: List[VendedorSchema]
    dataset2_clientes: List[ProdutoSchema]
    dataset3_itens: List[ItemPedidoSchema]
    dataset4_pedidos: List[PedidoSchema]
```

### **Schemas de Saída (limpos)**

Após processamento, retornam:

```python
class AllDatasetsLimpos:
    vendedores
    produtos
    transacoes
    pedidos
```

O schema de pedidos inclui métricas derivadas como:

* tempo de entrega
* tempo estimado
* atraso/adiantamento
* indicador de entrega no prazo

---

## 3. Limpeza e Validação (`services/`)

### **clean_vendedores.py**

* padronização de cidade e estado
* conversão de CEP para numérico
* preenchimento de nulos via mediana

### **clean_produtos.py**

* conversão de colunas numéricas
* tratamento de nulos
* padronização de categoria

### **clean_itens.py**

Implementa validação referencial entre tabelas:

✔ `order_id` existe em pedidos
✔ `product_id` existe em produtos
✔ `seller_id` existe em vendedores

Itens órfãos são automaticamente removidos.
Além disso:

* conversão de valores numéricos
* conversão de datas
* preenchimento por mediana

### **clean_pedidos.py**

Limpeza avançada:

* padronização de timestamps
* imputação baseada em mediana
* padronização de status (inglês → português)
* criação de colunas derivadas

  * `tempo_entrega_dias`
  * `tempo_entrega_estimado_dias`
  * `diferenca_entrega_dias`
  * `entrega_no_prazo`

---

## 4. Persistência (`data_saver.py`)

Todos os datasets são salvos na camada *Stage*:

```
data/stage_vendedores.csv
data/stage_produtos.csv
data/stage_itens_pedidos.csv
data/stage_pedidos.csv
```

A persistência é realizada em paralelo com:

```python
await asyncio.to_thread(...)
```

---

# Integração com o n8n

O pipeline automatizado funciona assim:

## **INPUT**

* Leitura de dados brutos de Google Sheets:

  * vendedores
  * produtos
  * itens
  * pedidos

## **PROCESS**

* Montagem do payload no formato da API
* Envio via POST para `/process-dataset`
* Validação do retorno
* Rota de fallback para erros:

  * registro em “failed_records”
  * envio de e-mail de falha

## **OUTPUT**

* Limpeza das 4 planilhas destino
* Inserção dos dados limpos
* Registro em `executions_log`
* E-mail de confirmação

Esse fluxo garante um **ETL 100% automatizado**.

---

# Como Rodar o Projeto

## **1. Instalar dependências**

Se houver `requirements.txt`:

```bash
pip install -r requirements.txt
```

Posso gerar este arquivo se precisar.

---

## **2. Rodar com Uvicorn**

```bash
uvicorn app.main:app --reload
```

A API ficará disponível em:

```
http://127.0.0.1:8000
```

---

# Testando o Endpoint

### **POST**

`http://127.0.0.1:8000/process-dataset`

### Exemplo de body:

```json
{
  "dataset1_vendedores": [...],
  "dataset2_clientes": [...],
  "dataset3_itens": [...],
  "dataset4_pedidos": [...]
}
```

### Retorno esperado:

```json
{
  "vendedores": [...],
  "produtos": [...],
  "transacoes": [...],
  "pedidos": [...]
}
```
