from fastapi import FastAPI
from app.routers import data_processing

app = FastAPI(
    title="API de Tratamento de Dados - Desafio 1",
    description="API que recebe dados brutos, os trata e os devolve limpos.",
    version="1.0.0",
)

# Todas as rotas (/, /health, /process-dataset) estão no router
app.include_router(data_processing.router)
