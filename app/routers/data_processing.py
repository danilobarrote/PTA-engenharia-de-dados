from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import List

from ..schemas.data_schemas import (
    PedidoSchema, PedidoLimpoSchema,
    ProdutoSchema, ProdutosLimpoSchema,
    VendedorSchema, VendedorLimpoSchema,
    ItemPedidoSchema, ItemPedidoLimpoSchema
)
from ..services.clean_pedidos import clean_single_pedido
from ..services.clean_produtos import clean_single_produto
from ..services.clean_vendedores import clean_single_vendedor
from ..services.clean_itens import clean_single_item
from ..services.full_sheet_cleanup import run_full_cleanup

router = APIRouter()

# ---------------------------------------------------------
# PEDIDOS
# ---------------------------------------------------------
@router.post("/clean/pedido", response_model=List[PedidoLimpoSchema])
def clean_pedidos_batch(payload: List[PedidoSchema]):
    # Aceita: [ { "order_id": "...", ... }, { ... } ]
    resultados = []
    try:
        for item in payload:
            resultados.append(clean_single_pedido(item))
        return resultados
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro em Pedidos: {str(e)}")

# ---------------------------------------------------------
# PRODUTOS (É AQUI QUE SEU JSON DEVE BATER)
# ---------------------------------------------------------
@router.post("/clean/produto", response_model=List[ProdutosLimpoSchema])
def clean_produtos_batch(payload: List[ProdutoSchema]):
    # Aceita: [ { "product_id": "...", "product_category_name": "..." } ]
    resultados = []
    try:
        for item in payload:
            resultados.append(clean_single_produto(item))
        return resultados
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro em Produtos: {str(e)}")

# ---------------------------------------------------------
# VENDEDORES
# ---------------------------------------------------------
@router.post("/clean/vendedor", response_model=List[VendedorLimpoSchema])
def clean_vendedores_batch(payload: List[VendedorSchema]):
    resultados = []
    try:
        for item in payload:
            resultados.append(clean_single_vendedor(item))
        return resultados
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro em Vendedores: {str(e)}")

# ---------------------------------------------------------
# ITENS DE PEDIDO
# ---------------------------------------------------------
@router.post("/clean/item", response_model=List[ItemPedidoLimpoSchema])
def clean_itens_batch(payload: List[ItemPedidoSchema]):
    resultados = []
    try:
        for item in payload:
            resultados.append(clean_single_item(item))
        return resultados
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro em Itens: {str(e)}")

# ---------------------------------------------------------
# TRIGGER
# ---------------------------------------------------------
@router.post("/trigger-full-cleanup")
def trigger_cleanup(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_full_cleanup)
    return {"message": "Full cleanup started in background"}