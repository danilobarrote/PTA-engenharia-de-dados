from fastapi import FastAPI
from app.routers import data_processing
from app.services.full_sheet_cleanup import run_full_cleanup_async

app = FastAPI(
    title="API de Tratamento de Dados - Desafio 1",
    description="API que recebe dados brutos, os trata e os devolve limpos.",
    version="1.0.0",
)

@app.on_event("startup")
async def startup_cleanup():
    """
    Executa o processo completo de limpeza de planilhas
    assim que a API é inicializada.
    """
    try:
        await run_full_cleanup_async()
    except Exception as e:
        # Se a limpeza falhar (e.g., credenciais do Google Sheets), 
        # a API continuará rodando, mas é bom registrar o erro.
        print(f"❌ ERRO durante o processo de limpeza no startup: {e}")
        # Você pode levantar 'raise' se quiser que a API falhe ao iniciar em caso de erro.

# Todas as rotas (/, /health, /process-dataset) estão no router
app.include_router(data_processing.router)
