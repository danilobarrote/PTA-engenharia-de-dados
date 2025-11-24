from fastapi import APIRouter, HTTPException
import asyncio

from app.services.data_saver import (
    process_and_persist_vendedores,
    process_and_persist_produtos,
    process_and_persist_itens,
)

from app.schemas.data_schemas import (
    SchemaRecepcaoDatasets,
    AllDatasetsLimpos,
)

router = APIRouter()


@router.post(
    "/process-dataset",
    response_model=AllDatasetsLimpos,
    summary="Recebe os 4 datasets brutos, limpa, persiste e retorna os dados enriquecidos.",
)
async def process_raw_datasets(datasets: SchemaRecepcaoDatasets):

    # Processa em paralelo apenas vendedores, produtos e itens
    results = await asyncio.gather(
        process_and_persist_vendedores(datasets.dataset1_vendedores),
        process_and_persist_produtos(datasets.dataset2_clientes),
        process_and_persist_itens(datasets.dataset3_itens),
        return_exceptions=True,
    )

    vendedores_limpos, produtos_limpos, transacoes_limpas = results

    # Pedidos ficam brutos mesmo – responsabilidade do Gabriel
    pedidos_brutos = datasets.dataset4_pedidos

    errors = [str(r) for r in results if isinstance(r, Exception)]
    if errors:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Erro na persistência de um ou mais datasets.",
                "errors": errors,
            },
        )

    return AllDatasetsLimpos(
        vendedores=vendedores_limpos,
        produtos=produtos_limpos,
        transacoes=transacoes_limpas,
        pedidos=pedidos_brutos,
    )


@router.get("/", description="Mensagem de boas-vindas da API.")
async def read_root():
    return {"message": "Bem-vindo à API de Tratamento de Dados!"}


@router.get("/health", description="Verifica a saúde da API.")
async def health_check():
    return {"status": "ok"}
