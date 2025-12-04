import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os

from .clean_pedidos import clean_pedidos_dataframe
from .clean_produtos import clean_produtos_dataframe
from .clean_vendedores import clean_vendedores_dataframe
from .clean_itens import clean_itens_dataframe
from .data_saver import save_df_to_sheet

# Configuração do Google
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = "credentials.json"  # Use the credentials.json from project root
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "15lX2IyBm3PZxoPA4a9r9LEJq81-6fI3qRYwuivDtSN8")

def get_gspread_client():
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"{CREDENTIALS_FILE} not found. Please ensure it exists in the project root.")
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)

def run_full_cleanup():
    print("Iniciando limpeza completa...")
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    # 1. Carregar Dados
    print("Lendo abas...")
    ws_orders = sh.worksheet("pedidos")
    ws_items = sh.worksheet("itens_pedidos")
    ws_products = sh.worksheet("produtos")
    ws_sellers = sh.worksheet("vendedores")

    df_orders = pd.DataFrame(ws_orders.get_all_records())
    df_items = pd.DataFrame(ws_items.get_all_records())
    df_products = pd.DataFrame(ws_products.get_all_records())
    df_sellers = pd.DataFrame(ws_sellers.get_all_records())

    # 2. Aplicar Limpezas Individuais
    print("Aplicando regras de negócio...")
    df_orders_clean = clean_pedidos_dataframe(df_orders)
    df_products_clean = clean_produtos_dataframe(df_products)
    df_sellers_clean = clean_vendedores_dataframe(df_sellers)
    df_items_clean = clean_itens_dataframe(df_items)

    # 3. Integridade Referencial (Regra Chave)
    print("Verificando integridade referencial...")
    
    # IDs válidos
    valid_orders = set(df_orders_clean['order_id'])
    valid_products = set(df_products_clean['product_id'])
    valid_sellers = set(df_sellers_clean['seller_id'])

    # Filtrar Itens Órfãos
    initial_items_count = len(df_items_clean)
    
    df_items_clean = df_items_clean[
        df_items_clean['order_id'].isin(valid_orders) &
        df_items_clean['product_id'].isin(valid_products) &
        df_items_clean['seller_id'].isin(valid_sellers)
    ]
    
    final_items_count = len(df_items_clean)
    print(f"Itens removidos por falha de integridade: {initial_items_count - final_items_count}")

    # 4. Salvar de volta
    print("Salvando dados limpos...")
    save_df_to_sheet(ws_orders, df_orders_clean)
    save_df_to_sheet(ws_products, df_products_clean)
    save_df_to_sheet(ws_sellers, df_sellers_clean)
    save_df_to_sheet(ws_items, df_items_clean)

    print("Limpeza completa finalizada com sucesso.")
    return {"status": "success", "removed_items": initial_items_count - final_items_count}