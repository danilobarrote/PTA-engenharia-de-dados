import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import numpy as np
from datetime import datetime

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

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

CREDENTIALS_FILE = "credentials.json"
SPREADSHEET_ID = "15lX2IyBm3PZxoPA4a9r9LEJq81-6fI3qRYwuivDtSN8"


def get_gsheet_client():
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE, SCOPE
    )
    return gspread.authorize(creds)


# ============================
# 2) Funções auxiliares
# ============================

def worksheet_to_df(ws):
    records = ws.get_all_records()
    return pd.DataFrame(records)


def df_to_worksheet(ws, df: pd.DataFrame):
    ws.clear()
    df_clean = df.where(pd.notnull(df), "")
    rows = [df_clean.columns.tolist()] + df_clean.astype(str).values.tolist()
    ws.update(rows)


# 🔥 Função universal para converter valores de DataFrame → Pydantic
def normalize_df_for_pydantic(df: pd.DataFrame, datetime_cols: list):
    df = df.where(df.notnull(), None)

    for col in datetime_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (
                    x.to_pydatetime()
                    if isinstance(x, pd.Timestamp)
                    else x if isinstance(x, datetime)
                    else None
                )
            )

    # conversão de numpy types → tipos nativos python
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: (
                x.item()
                if hasattr(x, "item")
                else x
            )
            if x is not None
            else None
        )

    return df


# ============================
# 3) Função principal de faxina
# ============================

def run_full_cleanup():

    def df_to_schema_list(df: pd.DataFrame, SchemaCls):

        df = df.replace({"": None})

        # ============ COLUNAS DE DATA ============

        if SchemaCls is PedidoSchema:
            datetime_cols = [
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ]
        elif SchemaCls is ItemPedidoSchema:
            datetime_cols = ["shipping_limit_date"]
        else:
            datetime_cols = []

        # Converte colunas de data
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Normaliza para datetime.datetime ou None
        df = normalize_df_for_pydantic(df, datetime_cols)

        # Monta para Pydantic
        records = df.to_dict("records")
        return [SchemaCls(**row) for row in records]

    # --- ORQUESTRAÇÃO ---

    client = get_gsheet_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    ws_pedidos = sh.worksheet("pedidos")
    ws_produtos = sh.worksheet("produtos")
    ws_vendedores = sh.worksheet("vendedores")
    ws_itens = sh.worksheet("itens_pedidos")

    df_pedidos = worksheet_to_df(ws_pedidos)
    df_produtos = worksheet_to_df(ws_produtos)
    df_vendedores = worksheet_to_df(ws_vendedores)
    df_itens = worksheet_to_df(ws_itens)

    vendedores_raw = df_to_schema_list(df_vendedores, VendedorSchema)
    produtos_raw = df_to_schema_list(df_produtos, ProdutoSchema)
    itens_raw = df_to_schema_list(df_itens, ItemPedidoSchema)
    pedidos_raw = df_to_schema_list(df_pedidos, PedidoSchema)

    pedidos_limpos = clean_pedidos(pedidos_raw)
    vendedores_limpos = clean_vendedores(vendedores_raw)
    produtos_limpos = clean_produtos(produtos_raw)

    df_pedidos_ref = pd.DataFrame([p.model_dump() for p in pedidos_limpos])
    df_produtos_ref = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_vendedores_ref = pd.DataFrame([v.model_dump() for v in vendedores_limpos])

    itens_limpos = clean_itens(
        itens_raw,
        df_pedidos=df_pedidos_ref,
        df_produtos=df_produtos_ref,
        df_vendedores=df_vendedores_ref,
    )

    df_pedidos_limpos = pd.DataFrame([p.model_dump() for p in pedidos_limpos])
    df_produtos_limpos = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_vendedores_limpos = pd.DataFrame([v.model_dump() for v in vendedores_limpos])
    df_itens_limpos = pd.DataFrame([i.model_dump() for i in itens_limpos])

    df_to_worksheet(ws_pedidos, df_pedidos_limpos)
    df_to_worksheet(ws_produtos, df_produtos_limpos)
    df_to_worksheet(ws_vendedores, df_vendedores_limpos)
    df_to_worksheet(ws_itens, df_itens_limpos)

    print("Faxina completa concluída!")


async def run_full_cleanup_async():
    print("🎬 Iniciando faxina...")
    await asyncio.to_thread(run_full_cleanup)
    print("✔ Finalizado!")
    

if __name__ == "__main__":
    run_full_cleanup()
