import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import asyncio

from app.services.clean_vendedores import clean_vendedores
from app.services.clean_produtos import clean_produtos
from app.services.clean_itens import clean_itens
from app.services.clean_pedidos import clean_pedidos


from app.schemas.data_schemas import (
    VendedorSchema,
    ProdutoSchema,
    ItemPedidoSchema,
    PedidoSchema,
)

# ============================
# 1) Configuração do Google
# ============================

# Escopos de acesso ao Google Sheets
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


CREDENTIALS_FILE = "credentials.json"

# ID da planilha (retirado da URL do Google Sheets)
SPREADSHEET_ID = "15lX2IyBm3PZxoPA4a9r9LEJq81-6fI3qRYwuivDtSN8"


def get_gsheet_client():
    """Cria o cliente autenticado do Google Sheets."""
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE, SCOPE
    )
    client = gspread.authorize(creds)
    return client


# ============================
# 2) Funções auxiliares
# ============================

def worksheet_to_df(ws):
    """Converte uma aba (worksheet) do Google Sheets para DataFrame."""
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    return df


def df_to_worksheet(ws, df: pd.DataFrame):
    """Limpa a aba e escreve o DataFrame como conteúdo novo."""
    ws.clear()
    # Cabeçalho + linhas (tudo convertido para string, vazios como "")
    rows = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    ws.update(rows)


# ============================
# 3) Função principal de faxina
# ============================

def run_full_cleanup():
    """
    Orquestra a leitura das planilhas, a aplicação das funções de limpeza
    e a escrita dos dados limpos de volta.
    """

    def df_to_schema_list(df: pd.DataFrame, SchemaCls):
        """
        Converte um DataFrame em lista de Schemas Pydantic,
        trocando "" e NaN por None e garantindo o tipo datetime para o Pandas.
        """
        from app.schemas.data_schemas import VendedorSchema, ItemPedidoSchema, PedidoSchema

        # 1) trocar strings vazias por None
        df = df.replace({"": None})

        # 2) Ajustes específicos por Schema (garantindo datetime para cálculos)
        if SchemaCls is VendedorSchema:
            if "seller_city" in df.columns:
                df["seller_city"] = df["seller_city"].astype(str)
            if "seller_state" in df.columns:
                df["seller_state"] = df["seller_state"].astype(str)

        # Para ItemPedidoSchema
        if SchemaCls is ItemPedidoSchema:
            if "shipping_limit_date" in df.columns:
                df["shipping_limit_date"] = pd.to_datetime(
                    df["shipping_limit_date"],
                    errors="coerce",
                )

        # Para PedidoSchema
        if SchemaCls is PedidoSchema:
            date_cols = [
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ]
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(
                        df[col],
                        errors="coerce",
                    )

        # 3) trocar NaN/NaT por None, depois dos ajustes
        df = df.where(pd.notnull(df), None)

        # 4) montar lista de dicts e depois Schemas
        records = df.to_dict("records")
        return [SchemaCls(**row) for row in records]


    # --- INÍCIO DA ORQUESTRAÇÃO ---

    client = get_gsheet_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    # Nomes das abas exatamente como estão na planilha
    ws_pedidos    = sh.worksheet("pedidos")
    ws_produtos   = sh.worksheet("produtos")
    ws_vendedores = sh.worksheet("vendedores")
    ws_itens      = sh.worksheet("itens_pedidos")

    # -------- 1) Ler para DataFrame (Leitura Bruta) --------
    df_pedidos    = worksheet_to_df(ws_pedidos)
    df_produtos   = worksheet_to_df(ws_produtos)
    df_vendedores = worksheet_to_df(ws_vendedores)
    df_itens      = worksheet_to_df(ws_itens)


    # -------- 2) Converter DF -> lista de Schemas (Prepara os dados para a limpeza) --------
    vendedores_raw = df_to_schema_list(df_vendedores, VendedorSchema)
    produtos_raw   = df_to_schema_list(df_produtos,   ProdutoSchema)
    itens_raw      = df_to_schema_list(df_itens,      ItemPedidoSchema)
    pedidos_raw    = df_to_schema_list(df_pedidos,    PedidoSchema)


    # -------- 3) Aplicar limpezas existentes (Onde a atuação ocorre) --------
    pedidos_limpos = clean_pedidos(pedidos_raw)
    vendedores_limpos = clean_vendedores(vendedores_raw)
    produtos_limpos = clean_produtos(produtos_raw)

    # DataFrames de referência para limpar itens (excluir órfãos)
    # NOTA: Estes DFs já contêm as strings DD-MM-YYYY nas colunas de data (se clean_pedidos for a atuadora).
    df_pedidos_ref = pd.DataFrame([p.model_dump() for p in pedidos_limpos])
    df_produtos_ref = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_vendedores_ref = pd.DataFrame([v.model_dump() for v in vendedores_limpos])

    itens_limpos = clean_itens(
        itens_raw,
        df_pedidos=df_pedidos_ref,
        df_produtos=df_produtos_ref,
        df_vendedores=df_vendedores_ref,
    )

    # -------- 4) Converter de volta para DataFrame (Preparação para Escrita) --------
    df_pedidos_limpos = pd.DataFrame([p.model_dump() for p in pedidos_limpos])
    df_produtos_limpos = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_vendedores_limpos = pd.DataFrame([v.model_dump() for v in vendedores_limpos])
    df_itens_limpos = pd.DataFrame([i.model_dump() for i in itens_limpos])

    # -------- 5) Escrever de volta nas abas --------
    df_to_worksheet(ws_pedidos, df_pedidos_limpos)
    df_to_worksheet(ws_produtos, df_produtos_limpos)
    df_to_worksheet(ws_vendedores, df_vendedores_limpos)
    df_to_worksheet(ws_itens, df_itens_limpos)

    print("Faxina completa concluída com sucesso!")


async def run_full_cleanup_async():
    """Wrapper assíncrono para rodar limpeza de planilhas em thread separado."""
    print("🎬 Iniciando a faxina completa de planilhas (em thread separado)...")
    await asyncio.to_thread(run_full_cleanup)
    print("✅ Faxina de planilhas concluída.")

if __name__ == "__main__":
    run_full_cleanup()