from fastapi import FastAPI
import uvicorn
from app.routers import example_router

from app.schemas.data_schemas import (
    SchemaRecepcaoDatasets, 
    RespostaStatusProcessamento
)

app = FastAPI(
    title="API de Tratamento de Dados - Desafio 1",
    description="API que recebe dados brutos, os trata e os devolve limpos.",
    version="1.0.0"
)

@app.get("/", description="Mensagem de boas-vindas da API.")
async def read_root():
    return {"message": "Bem-vindo à API de Tratamento de Dados!"}

@app.get("/health", description="Verifica a saúde da API.")
async def health_check():
    return {"status": "ok"}


@app.post(
    "/process-dataset",
    response_model=RespostaStatusProcessamento,
    summary="Receber e iniciar o tratamento dos 4 datasets brutos."
)
async def process_raw_datasets(datasets: SchemaRecepcaoDatasets):

    total_records = (
        len(datasets.dataset1_vendedores) +
        len(datasets.dataset2_clientes) +
        len(datasets.dataset3_transacoes) +
        len(datasets.dataset4_pedidos)
    )
    
    return RespostaStatusProcessamento(
        total_records_processed=total_records
    )