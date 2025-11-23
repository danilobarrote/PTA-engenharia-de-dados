from fastapi import APIRouter, HTTPException
import asyncio

from app.services.data_saver import (
    process_and_persist_vendedores,
    process_and_persist_produtos,
    process_and_persist_itens,
    process_and_persist_pedidos
)

from app.schemas.data_schemas import (
    SchemaRecepcaoDatasets, 
    AllDatasetsLimpos 
)

router = APIRouter()

@router.post(
    "/process-dataset",
    response_model=AllDatasetsLimpos, 
    summary="Recebe os 4 datasets brutos, limpa, persiste e retorna os dados enriquecidos."
)

async def process_raw_datasets(datasets: SchemaRecepcaoDatasets):
    
    # Inicia as quatro tarefas de persistência *simultaneamente*
    results = await asyncio.gather(
        process_and_persist_vendedores(datasets.dataset1_vendedores),
        process_and_persist_produtos(datasets.dataset2_clientes),
        process_and_persist_itens(datasets.dataset3_itens),
        process_and_persist_pedidos(datasets.dataset4_pedidos),
        return_exceptions=True # Garante que a falha em um dataset não interrompa os outros
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

    vendedores_limpos, produtos_limpos, transacoes_limpas, pedidos_limpos = results

    # Retorno JSON final
    return AllDatasetsLimpos(
        vendedores=vendedores_limpos,
        produtos=produtos_limpos,
        transacoes=transacoes_limpas,
        pedidos=pedidos_limpos
    )

@router.get("/", description="Mensagem de boas-vindas da API.")
async def read_root():
    return {"message": "Bem-vindo à API de Tratamento de Dados!"}

@router.get("/health", description="Verifica a saúde da API.")
async def health_check():
    return {"status": "ok"}