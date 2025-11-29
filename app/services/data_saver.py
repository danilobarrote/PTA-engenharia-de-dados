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
# Configurações de Caminhos (Simulando Tables)
# -------------------------------------------------
DATA_DIR = "data"
PATH_VENDEDORES = f"{DATA_DIR}/stage_vendedores.csv"
PATH_PRODUTOS = f"{DATA_DIR}/stage_produtos.csv"
PATH_PEDIDOS = f"{DATA_DIR}/stage_pedidos.csv"
PATH_ITENS = f"{DATA_DIR}/stage_itens_pedidos.csv"

# -------------------------------------------------
# Helper: garantir que a pasta data/ exista
# -------------------------------------------------
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

# -------------------------------------------------
# Helper: Carregar Referências (Leitura do Stage)
# -------------------------------------------------
def _load_ref_dataframe(file_path: str, id_column: str) -> pd.DataFrame:
    """
    Tenta carregar um CSV existente. 
    Se não existir, retorna um DataFrame vazio com a coluna de ID
    para garantir que a validação não quebre (resultando em 0 validados).
    """
    if os.path.exists(file_path):
        try:
            # Lê o CSV garantindo que IDs sejam strings para comparação correta
            df = pd.read_csv(file_path, dtype={id_column: str})
            return df
        except Exception as e:
            print(f"ERRO ao ler referência em {file_path}: {e}")
            return pd.DataFrame(columns=[id_column])
    
    print(f"AVISO: Arquivo de referência {file_path} não encontrado. Assumindo vazio.")
    return pd.DataFrame(columns=[id_column])

# -------------------------------------------------
# Funções síncronas de persistência (Stage mock)
# -------------------------------------------------

def save_vendedores_sync(records: List[VendedorSchema]) -> int:
    ensure_data_dir()
    if not records:
        print("AVISO: Lista de Vendedores vazia. Pulando a persistência.")
        return 0

    vendedores_dicts = [r.model_dump() for r in records] 
    df_vendedores = pd.DataFrame(vendedores_dicts)
    df_vendedores.to_csv(PATH_VENDEDORES, index=False)
    print(f"INFO: Vendedores persistidos em {PATH_VENDEDORES}")
    return len(records)

def save_produtos_sync(records: List[ProdutoSchema]) -> int:
    ensure_data_dir()
    if not records:
        print("AVISO: Lista de Produtos vazia. Pulando a persistência.")
        return 0

    produtos_dicts = [r.model_dump() for r in records]
    df_produtos = pd.DataFrame(produtos_dicts)
    df_produtos.to_csv(PATH_PRODUTOS, index=False)
    print(f"INFO: Produtos persistidos em {PATH_PRODUTOS}")
    return len(records)

def save_pedidos_sync(records: List[PedidoLimpoSchema]) -> int:
    ensure_data_dir()
    if not records:
        print("AVISO: Lista de Pedidos vazia. Pulando a persistência.")
        return 0

    pedidos_dicts = [r.model_dump() for r in records]
    df_pedidos = pd.DataFrame(pedidos_dicts)
    df_pedidos.to_csv(PATH_PEDIDOS, index=False)
    print(f"INFO: Pedidos persistidos em {PATH_PEDIDOS}")
    return len(records)

def save_itens_sync(records: List[ItemPedidoSchema]) -> int:
    ensure_data_dir()
    if not records:
        print("AVISO: Lista de Itens vazia. Pulando a persistência.")
        return 0

    transacoes_dicts = [r.model_dump() for r in records] 
    df_transacoes = pd.DataFrame(transacoes_dicts)
    df_transacoes.to_csv(PATH_ITENS, index=False)
    print(f"INFO: Itens de Pedidos persistidos em {PATH_ITENS}")
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

async def process_and_persist_pedidos(raw_records: List[PedidoSchema]) -> List[PedidoLimpoSchema]:
    cleaned_records = await asyncio.to_thread(clean_pedidos, raw_records)
    await asyncio.to_thread(save_pedidos_sync, cleaned_records)
    return cleaned_records

async def process_and_persist_itens(raw_records: List[ItemPedidoSchema]) -> List[ItemPedidoLimpoSchema]:
    """
    Processa itens recuperando as referências (FKs) dos arquivos CSV já salvos.
    """
    
    # 1. Carrega as tabelas de referência do "Banco de Dados" (CSV)
    # Usamos asyncio.to_thread para não bloquear o loop principal com I/O de disco
    df_pedidos_ref = await asyncio.to_thread(_load_ref_dataframe, PATH_PEDIDOS, "order_id")
    df_produtos_ref = await asyncio.to_thread(_load_ref_dataframe, PATH_PRODUTOS, "product_id")
    df_vendedores_ref = await asyncio.to_thread(_load_ref_dataframe, PATH_VENDEDORES, "seller_id")

    # 2. Executa a limpeza passando as referências carregadas
    cleaned_records = await asyncio.to_thread(
        clean_itens, 
        raw_records, 
        df_pedidos_ref, 
        df_produtos_ref, 
        df_vendedores_ref
    )

    # 3. Salva os itens limpos
    await asyncio.to_thread(save_itens_sync, cleaned_records)
    
    return cleaned_records