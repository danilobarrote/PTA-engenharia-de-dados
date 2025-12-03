from fastapi import FastAPI
from routers import data_processing

app = FastAPI(
    title="API de Tratamento de Dados - O-Market",
    description="API para limpeza e padronização de dados do pipeline ETL.",
    version="1.0.0"
)

app.include_router(data_processing.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"status": "online", "message": "API de Engenharia de Dados O-Market rodando."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)