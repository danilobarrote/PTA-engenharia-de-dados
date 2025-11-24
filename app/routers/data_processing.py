from fastapi import APIRouter, HTTPException
import asyncio
import pandas as pd

from app.services.data_saver import (
    process_and_persist_vendedores,
    process_and_persist_produtos,
    process_and_persist_itens,
    process_and_persist_pedidos,
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

    # ----------------------------------------------------
    # 1. Preparação dos DataFrames de Referência 🛠️
    # ----------------------------------------------------
    
    # 1.1) Converte os datasets de pedidos, produtos e vendedores para DataFrames.
    
    df_pedidos_ref = pd.DataFrame([p.model_dump() for p in datasets.dataset4_pedidos])
    
    df_produtos_ref = pd.DataFrame([p.model_dump() for p in datasets.dataset2_clientes]) 
    
    df_vendedores_ref = pd.DataFrame([v.model_dump() for v in datasets.dataset1_vendedores])

    # ----------------------------------------------------
    # 2. Processamento (Execução em Paralelo) 🔄
    # ----------------------------------------------------

    results = await asyncio.gather(
        process_and_persist_vendedores(datasets.dataset1_vendedores),
        process_and_persist_produtos(datasets.dataset2_clientes),
        process_and_persist_itens(
            datasets.dataset3_itens,
            df_pedidos=df_pedidos_ref,
            df_produtos=df_produtos_ref,
            df_vendedores=df_vendedores_ref,),
        process_and_persist_pedidos(datasets.dataset4_pedidos),
        return_exceptions=True,
    )

    vendedores_limpos, produtos_limpos, transacoes_limpas, pedidos_limpos = results


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
        pedidos=pedidos_limpos,
    )


@router.get("/", description="Mensagem de boas-vindas da API.")
async def read_root():
    return {"message": "Bem-vindo à API de Tratamento de Dados!"}


@router.get("/health", description="Verifica a saúde da API.")
async def health_check():
    return {"status": "ok"}
