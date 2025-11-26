---

# PTA – Engenharia de Dados

## API de Tratamento, Validação e Padronização de Datasets + Pipeline de Limpeza de Google Sheets

Este projeto implementa uma API em FastAPI responsável por receber quatro datasets brutos provenientes de uma automação no n8n, realizar limpeza, padronização, validação cruzada, enriquecimento de dados e persistência em uma camada Stage (mock). Após o processamento, a API retorna os datasets tratados, prontos para uso analítico.

Além da API, o projeto inclui um pipeline completo para limpeza e padronização dos dados armazenados no Google Sheets, permitindo manter o DW sempre consistente e sincronizado com o pipeline da API.

O objetivo é fornecer uma solução completa de ETL automatizado, garantindo consistência entre tabelas, correção de valores inconsistentes, padronização de datas e status, além de derivação de indicadores úteis para análise e tomada de decisão.

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
 │    ├── data_saver.py
 │    └── full_sheet_cleanup.py        ← limpeza completa do Google Sheets
data/
 ├── stage_vendedores.csv
 ├── stage_produtos.csv
 ├── stage_itens_pedidos.csv
 └── stage_pedidos.csv
credentials.json   ← ignorado pelo Git
```

A arquitetura segue um padrão modular:

* routers/: definição de endpoints
* schemas/: modelos de entrada e saída
* services/: regras de limpeza, validação e persistência
* data/: camada stage para armazenar os CSVs tratados
* full_sheet_cleanup.py: pipeline de limpeza das abas do Google Sheets (DW)

---

# Componentes e Funcionalidades

## 1. Router (data_processing.py)

### POST /process-dataset

Executa o pipeline completo:

* recebe os quatro datasets brutos
* converte tudo para DataFrames
* executa limpeza e validação
* padroniza datas, status e tipos
* remove registros órfãos
* enriquece os dados
* persiste em CSVs da camada Stage
* retorna todos os datasets enriquecidos

O processamento usa concorrência com:

```
asyncio.gather(...)
```

### GET /

Mensagem simples de boas-vindas.

### GET /health

Health check da aplicação.

---

## 2. Schemas (data_schemas.py)

### Schemas de Entrada (brutos)

* VendedorSchema
* ProdutoSchema
* ItemPedidoSchema
* PedidoSchema

Estrutura esperada:

```
class SchemaRecepcaoDatasets:
    dataset1_vendedores: List[VendedorSchema]
    dataset2_clientes: List[ProdutoSchema]
    dataset3_itens: List[ItemPedidoSchema]
    dataset4_pedidos: List[PedidoSchema]
```

### Schemas de Saída (limpos)

Retorno consolidado:

```
class AllDatasetsLimpos:
    vendedores
    produtos
    transacoes
    pedidos
```

O schema de pedidos contém colunas derivadas:

* tempo_entrega_dias
* tempo_entrega_estimado_dias
* diferenca_entrega_dias
* entrega_no_prazo

---

# 3. Limpeza e Validação (services/)

## clean_vendedores.py

* padronização de cidade
* padronização e normalização de estado
* conversão e validação de CEP
* preenchimento de nulos

## clean_produtos.py

* conversão de colunas numéricas
* remoção de nulos
* padronização de categoria

## clean_itens.py

Implementa validação referencial entre tabelas:

* order_id deve existir em pedidos
* product_id deve existir em produtos
* seller_id deve existir em vendedores

Itens órfãos são removidos automaticamente.

Também são realizados:

* conversão de datas
* conversão de numéricos
* preenchimento por mediana

## clean_pedidos.py

Limpeza avançada com:

* padronização de timestamps
* suporte a múltiplos formatos de data (ISO, DD/MM/YYYY, YYYY-MM-DD)
* imputação de datas ausentes
* mapeamento de status (inglês → português) com fallback seguro
* criação de colunas derivadas:

  * tempo_entrega_dias
  * tempo_entrega_estimado_dias
  * diferenca_entrega_dias
  * entrega_no_prazo

Todos os valores finais são garantidos como válidos para os schemas Pydantic.

---

# 4. Persistência (data_saver.py)

Após tratada, cada tabela é salva em:

```
data/stage_vendedores.csv
data/stage_produtos.csv
data/stage_itens_pedidos.csv
data/stage_pedidos.csv
```

Persistência executada em paralelo via:

```
await asyncio.to_thread(...)
```

---

# 5. Pipeline de Limpeza do Google Sheets (full_sheet_cleanup.py)

Pipeline completo que:

1. Lê as quatro abas:

   * Vendedores
   * Produtos
   * ItensPedidos
   * Pedidos

2. Converte as linhas da planilha em schemas Pydantic.

3. Reutiliza exatamente as mesmas funções de limpeza usadas na API.

4. Reescreve as abas do Google Sheets com os dados limpos.

Este script garante que o DW no Google Sheets esteja sempre sincronizado com as regras oficiais da API.

Para executar:

```
python -m app.services.full_sheet_cleanup
```

É necessário possuir o arquivo `credentials.json` com acesso à planilha e informar o `SPREADSHEET_ID` correspondente.

---

# Integração com o n8n

Fluxo automatizado:

### Entrada

Leitura das abas da planilha (Google Sheets):

* vendedores
* produtos
* itens
* pedidos

### Processamento

* montagem do payload JSON
* chamada ao endpoint `/process-dataset`
* tratamento de erros
* fallback para logs e e-mails

### Saída

* limpeza das abas destino
* escrita dos dados limpos
* log em executions_log
* notificação final

---

# Como Rodar o Projeto

## 1. Instalar dependências

Se houver requirements.txt:

```
pip install -r requirements.txt
```

Caso precise, um arquivo pode ser gerado.

## 2. Rodar a API

```
uvicorn app.main:app --reload
```

A API ficará disponível em:

```
http://127.0.0.1:8000
```

Documentação interativa:

```
http://127.0.0.1:8000/docs
```

---

# Testando o Endpoint

POST

```
http://127.0.0.1:8000/process-dataset
```

Exemplo de body:

```
{
  "dataset1_vendedores": [...],
  "dataset2_clientes": [...],
  "dataset3_itens": [...],
  "dataset4_pedidos": [...]
}
```

Retorno esperado:

```
{
  "vendedores": [...],
  "produtos": [...],
  "transacoes": [...],
  "pedidos": [...]
}
```

---
