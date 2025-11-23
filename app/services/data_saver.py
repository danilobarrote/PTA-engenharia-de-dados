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
)

# -------------------------------------------------
# Helper: garantir que a pasta data/ exista
# -------------------------------------------------
def ensure_data_dir():
    os.makedirs("data", exist_ok=True)


# -------------------------------------------------
# Funções síncronas de persistência (Stage mock)
# -------------------------------------------------
def save_vendedores_sync(records: List[VendedorLimpoSchema]) -> int:
    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0

    ensure_data_dir()
    df = pd.DataFrame([r.model_dump() for r in records])
    output_path = "data/stage_vendedores.csv"
    df.to_csv(output_path, index=False)
    print(f"INFO: Vendedores persistidos em {output_path}")
    return len(records)


def save_produtos_sync(records: List[ProdutosLimpoSchema]) -> int:
    if not records:
        print("AVISO: Lista de Produtos vazia. Pulando a persistência.")
        return 0

    ensure_data_dir()
    df = pd.DataFrame([r.model_dump() for r in records])
    output_path = "data/stage_produtos.csv"
    df.to_csv(output_path, index=False)
    print(f"INFO: Produtos persistidos em {output_path}")
    return len(records)


def save_itens_sync(records: List[ItemPedidoLimpoSchema]) -> int:
    if not records:
        print("AVISO: Lista de Itens vazia. Pulando a persistência.")
        return 0

    ensure_data_dir()
    df = pd.DataFrame([r.model_dump() for r in records])
    output_path = "data/stage_itens_pedidos.csv"
    df.to_csv(output_path, index=False)
    print(f"INFO: Itens de Pedido persistidos em {output_path}")
    return len(records)


# -------------------------------------------------
# Funções assíncronas de orquestração
# (limpeza mínima + persistência)
# -------------------------------------------------
async def process_and_persist_vendedores(
    raw_records: List[VendedorSchema],
) -> List[VendedorLimpoSchema]:
    # por enquanto, “limpeza” = apenas tipar como VendedorLimpoSchema
    cleaned_records = [VendedorLimpoSchema(**r.model_dump()) for r in raw_records]
    await asyncio.to_thread(save_vendedores_sync, cleaned_records)
    return cleaned_records


async def process_and_persist_produtos(
    raw_records: List[ProdutoSchema],
) -> List[ProdutosLimpoSchema]:
    cleaned_records = [ProdutosLimpoSchema(**r.model_dump()) for r in raw_records]
    await asyncio.to_thread(save_produtos_sync, cleaned_records)
    return cleaned_records


async def process_and_persist_itens(
    raw_records: List[ItemPedidoSchema],
) -> List[ItemPedidoLimpoSchema]:
    cleaned_records = [ItemPedidoLimpoSchema(**r.model_dump()) for r in raw_records]
    await asyncio.to_thread(save_itens_sync, cleaned_records)
    return cleaned_records
