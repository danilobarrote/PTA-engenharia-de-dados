import pandas as pd
import asyncio
from fastapi import FastAPI, HTTPException
from typing import List

# Importa todos os schemas necessários
from app.schemas.data_schemas import (
    SchemaRecepcaoDatasets, 
    RespostaStatusProcessamento,
    VendedorSchema,
    ProdutoSchema,
    ItemPedidoSchema, 
    PedidoSchema      
)

# Estas funções usam Pandas (código síncrono) e rodam em threads separadas.

def save_vendedores_sync(records: List[VendedorSchema]):

    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0
    
    """Converte e salva Vendedores na camada Stage (Mock)."""
    vendedores_dicts = [r.model_dump() for r in records] 
    df_vendedores = pd.DataFrame(vendedores_dicts)
    output_path = "data/stage_vendedores.csv" 
    df_vendedores.to_csv(output_path, index=False)
    print(f"INFO: Vendedores persistidos em {output_path}")
    return len(records)

def save_produtos_sync(records: List[ProdutoSchema]):

    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0
    
    """Converte e salva Produtos na camada Stage (Mock)."""
    produtos_dicts = [r.model_dump() for r in records]
    df_produtos = pd.DataFrame(produtos_dicts)
    output_path = "data/stage_produtos.csv" 
    df_produtos.to_csv(output_path, index=False)
    print(f"INFO: Produtos persistidos em {output_path}")
    return len(records)

def save_transacoes_sync(records: List[ItemPedidoSchema]):

    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0
    
    """Converte e salva Itens de Pedidos (Transações) na camada Stage (Mock)."""
    transacoes_dicts = [r.model_dump() for r in records] 
    df_transacoes = pd.DataFrame(transacoes_dicts)
    output_path = "data/stage_itens_pedidos.csv" 
    df_transacoes.to_csv(output_path, index=False)
    print(f"INFO: Itens de Pedidos persistidos em {output_path}")
    return len(records)

def save_pedidos_sync(records: List[PedidoSchema]):

    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0
    
    """Converte e salva Pedidos na camada Stage (Mock)."""
    pedidos_dicts = [r.model_dump() for r in records]
    df_pedidos = pd.DataFrame(pedidos_dicts)
    output_path = "data/stage_pedidos.csv" 
    df_pedidos.to_csv(output_path, index=False)
    print(f"INFO: Pedidos persistidos em {output_path}")
    return len(records)


# --- FUNÇÕES ASSÍNCRONAS DE ORQUESTRAÇÃO ---

async def persist_vendedores_to_stage(records: List[VendedorSchema]):
    return await asyncio.to_thread(save_vendedores_sync, records)

async def persist_produtos_to_stage(records: List[ProdutoSchema]):
    return await asyncio.to_thread(save_produtos_sync, records)

async def persist_transacoes_to_stage(records: List[ItemPedidoSchema]):
    return await asyncio.to_thread(save_transacoes_sync, records)

async def persist_pedidos_to_stage(records: List[PedidoSchema]):
    return await asyncio.to_thread(save_pedidos_sync, records)


app = FastAPI(
    title="API de Tratamento de Dados - Desafio 1",
    description="API que recebe dados brutos, os trata e os devolve limpos.",
    version="1.0.0"
)

@app.post(
    "/process-dataset",
    response_model=RespostaStatusProcessamento,
    summary="Receber e iniciar o tratamento dos 4 datasets brutos."
)
async def process_raw_datasets(datasets: SchemaRecepcaoDatasets):
    
    # Inicia as quatro tarefas de persistência *simultaneamente*
    results = await asyncio.gather(
        persist_vendedores_to_stage(datasets.dataset1_vendedores),
        persist_produtos_to_stage(datasets.dataset2_clientes),
        persist_transacoes_to_stage(datasets.dataset3_transacoes),
        persist_pedidos_to_stage(datasets.dataset4_pedidos),
        return_exceptions=True  # Garante que a falha em um dataset não interrompa os outros
    )

    # Processamento dos resultados para totalizar e verificar erros
    total_records = 0
    errors = []
    
    for r in results:
        if isinstance(r, int):
            total_records += r
        elif isinstance(r, Exception):
            errors.append(str(r))
    
    # Se houve erros, levanta uma exceção HTTP 500 para o cliente
    if errors:
         raise HTTPException(
             status_code=500,
             detail={
                 "message": "Erro na persistência concorrente de um ou mais datasets.",
                 "errors": errors
             }
         )

    # Retorno JSON final de sucesso
    return RespostaStatusProcessamento(
        total_records_processed=total_records,
        message="Os 4 datasets foram processados e persistidos na camada Stage (Mock) com sucesso."
    )

@app.get("/", description="Mensagem de boas-vindas da API.")
async def read_root():
    return {"message": "Bem-vindo à API de Tratamento de Dados!"}

@app.get("/health", description="Verifica a saúde da API.")
async def health_check():
    return {"status": "ok"}