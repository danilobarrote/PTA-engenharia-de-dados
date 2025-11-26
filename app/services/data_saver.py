import os
import pandas as pd
import asyncio
from typing import List

from app.schemas.data_schemas import (
    VendedorSchema,
    VendedorLimpoSchema,
    ProdutoSchema,
    ProdutosLimpoSchema,
    ItemPedidoSchema,
    ItemPedidoLimpoSchema,
    PedidoSchema,
    PedidoLimpoSchema 
)

from app.services.clean_pedidos import clean_pedidos
from app.services.clean_produtos import clean_produtos
from app.services.clean_itens import clean_itens
from app.services.clean_vendedores import clean_vendedores


# -------------------------------------------------
# Helper: garantir que a pasta data/ exista
# -------------------------------------------------
def ensure_data_dir():
    os.makedirs("data", exist_ok=True)

# -------------------------------------------------
# Funções síncronas de persistência (Stage mock)
# -------------------------------------------------
def save_vendedores_sync(records: List[VendedorSchema])-> int:

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

def save_produtos_sync(records: List[ProdutoSchema])-> int:

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

def save_itens_sync(records: List[ItemPedidoSchema]):

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

def save_pedidos_sync(records: List[PedidoLimpoSchema]) -> int:

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


# -------------------------------------------------
# Funções assíncronas de orquestração
# -------------------------------------------------
async def process_and_persist_vendedores(raw_records: List[VendedorSchema]) -> List[VendedorLimpoSchema]:
    cleaned_records = await asyncio.to_thread(clean_vendedores, raw_records)

    await asyncio.to_thread(save_vendedores_sync, cleaned_records)
    return cleaned_records

async def process_and_persist_produtos(raw_records: List[ProdutoSchema]) -> List[ProdutosLimpoSchema]:
    cleaned_records = await asyncio.to_thread(clean_produtos, raw_records)

    await asyncio.to_thread(save_produtos_sync, cleaned_records)
    return cleaned_records

async def process_and_persist_itens(
    raw_records: List[ItemPedidoSchema], 
    df_pedidos: pd.DataFrame,    
    df_produtos: pd.DataFrame,
    df_vendedores: pd.DataFrame) -> List[ItemPedidoLimpoSchema]:

    cleaned_records = await asyncio.to_thread(clean_itens, raw_records, df_pedidos, df_produtos, df_vendedores)

    await asyncio.to_thread(save_itens_sync, cleaned_records)
    return cleaned_records

async def process_and_persist_pedidos(raw_records: List[PedidoSchema]) -> List[PedidoLimpoSchema]:
    cleaned_records = await asyncio.to_thread(clean_pedidos, raw_records)

    await asyncio.to_thread(save_pedidos_sync, cleaned_records)
    return cleaned_records