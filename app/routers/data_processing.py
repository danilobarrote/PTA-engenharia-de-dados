from fastapi import APIRouter, HTTPException
from typing import List

# Importação dos serviços
from app.services.data_saver import (
    process_and_persist_vendedores,
    process_and_persist_produtos,
    process_and_persist_pedidos,
    process_and_persist_itens,
)

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

router = APIRouter()

# ----------------------------------------------------
# 1. Endpoint Vendedores
# ----------------------------------------------------
@router.post(
    "/process-vendedores",
    response_model=List[VendedorLimpoSchema],
    summary="Processa e persiste a lista de vendedores.",
    description="Recebe a lista bruta, limpa e atualiza a base de vendedores."
)
async def process_vendedores(vendedores: List[VendedorSchema]):
    try:
        return await process_and_persist_vendedores(vendedores)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro em vendedores: {str(e)}")


# ----------------------------------------------------
# 2. Endpoint Produtos
# ----------------------------------------------------
@router.post(
    "/process-produtos",
    response_model=List[ProdutosLimpoSchema],
    summary="Processa e persiste a lista de produtos.",
    description="Recebe a lista bruta, limpa e atualiza a base de produtos."
)
async def process_produtos(produtos: List[ProdutoSchema]):
    try:
        return await process_and_persist_produtos(produtos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro em produtos: {str(e)}")


# ----------------------------------------------------
# 3. Endpoint Pedidos
# ----------------------------------------------------
@router.post(
    "/process-pedidos",
    response_model=List[PedidoLimpoSchema],
    summary="Processa e persiste a lista de pedidos.",
    description="Recebe a lista bruta, calcula prazos e atualiza a base de pedidos."
)
async def process_pedidos(pedidos: List[PedidoSchema]):
    try:
        return await process_and_persist_pedidos(pedidos)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro em pedidos: {str(e)}")


# ----------------------------------------------------
# 4. Endpoint Itens de Pedidos
# ----------------------------------------------------
@router.post(
    "/process-itens",
    response_model=List[ItemPedidoLimpoSchema],
    summary="Processa e persiste a lista de itens de pedidos.",
    description=(
        "IMPORTANTE: Este endpoint depende que Vendedores, Produtos e Pedidos "
        "já tenham sido carregados no banco de dados. A validação de integridade "
        "(FKs) será feita consultando a base existente."
    )
)
async def process_itens(itens: List[ItemPedidoSchema]):
    try:
        # AQUI ESTÁ A MUDANÇA CONCEITUAL:
        #
        # O argumento 'df_pedidos', 'df_produtos', etc. não vem mais da request HTTP.
        #
        # A função 'process_and_persist_itens' agora deve ser responsável por:
        # 1. Conectar no Banco de Dados.
        # 2. Baixar os IDs válidos de Pedidos, Produtos e Vendedores (ex: select id from table).
        # 3. Passar esses DataFrames/Listas para a sua função 'clean_itens'.
        #
        # Dessa forma, mantemos a lógica de exclusão de linhas órfãs sem precisar
        # subir todos os arquivos novamente.
        
        resultado = await process_and_persist_itens(itens)
        return resultado

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar itens: {str(e)}"
        )


# ----------------------------------------------------
# Endpoints Utilitários
# ----------------------------------------------------
@router.get("/", description="Mensagem de boas-vindas.")
async def read_root():
    return {"message": "API de Tratamento de Dados - Endpoints Segregados"}

@router.get("/health")
async def health_check():
    return {"status": "ok"}