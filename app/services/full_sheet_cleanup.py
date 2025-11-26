import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

# Caminho do JSON de credenciais (ajuste esse caminho)
CREDENTIALS_FILE = "credentials.json"

# ID da planilha (pegue na URL do Google Sheets)
SPREADSHEET_ID = "COLOQUE_O_ID_DA_PLANILHA_AQUI"


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
    # Escrever cabeçalho + linhas
    rows = [df.columns.tolist()] + df.astype(str).fillna("").values.tolist()
    ws.update(rows)


# ============================
# 3) Função principal de faxina
# ============================

def run_full_cleanup():
    """
    Lê as 4 abas da planilha,
    aplica as limpezas já existentes
    e sobrescreve as abas com os dados limpos.
    """
    client = get_gsheet_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    # Ajuste os nomes das abas conforme a planilha real
    ws_vendedores = sh.worksheet("Vendedores")
    ws_produtos   = sh.worksheet("Produtos")
    ws_itens      = sh.worksheet("ItensPedidos")
    ws_pedidos    = sh.worksheet("Pedidos")

    # -------- 1) Ler para DataFrame --------
    df_vendedores = worksheet_to_df(ws_vendedores)
    df_produtos   = worksheet_to_df(ws_produtos)
    df_itens      = worksheet_to_df(ws_itens)
    df_pedidos    = worksheet_to_df(ws_pedidos)

    # -------- 2) Converter DF -> lista de Schemas --------
    vendedores_raw = [VendedorSchema(**row) for row in df_vendedores.to_dict("records")]
    produtos_raw   = [ProdutoSchema(**row)  for row in df_produtos.to_dict("records")]
    itens_raw      = [ItemPedidoSchema(**row) for row in df_itens.to_dict("records")]
    pedidos_raw    = [PedidoSchema(**row) for row in df_pedidos.to_dict("records")]

    # -------- 3) Aplicar limpezas existentes --------
    pedidos_limpos    = clean_pedidos(pedidos_raw)
    vendedores_limpos = clean_vendedores(vendedores_raw)
    produtos_limpos   = clean_produtos(produtos_raw)

    # DataFrames de referência para limpar itens (excluir órfãos)
    df_pedidos_ref    = pd.DataFrame([p.model_dump() for p in pedidos_limpos])
    df_produtos_ref   = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_vendedores_ref = pd.DataFrame([v.model_dump() for v in vendedores_limpos])

    itens_limpos = clean_itens(
        itens_raw,
        df_pedidos=df_pedidos_ref,
        df_produtos=df_produtos_ref,
        df_vendedores=df_vendedores_ref,
    )

    # -------- 4) Converter de volta para DataFrame --------
    df_vendedores_limpos = pd.DataFrame([v.model_dump() for v in vendedores_limpos])
    df_produtos_limpos   = pd.DataFrame([p.model_dump() for p in produtos_limpos])
    df_itens_limpos      = pd.DataFrame([i.model_dump() for i in itens_limpos])
    df_pedidos_limpos    = pd.DataFrame([p.model_dump() for p in pedidos_limpos])

    # -------- 5) Escrever de volta nas abas --------
    df_to_worksheet(ws_vendedores, df_vendedores_limpos)
    df_to_worksheet(ws_produtos,   df_produtos_limpos)
    df_to_worksheet(ws_itens,      df_itens_limpos)
    df_to_worksheet(ws_pedidos,    df_pedidos_limpos)

    print("Faxina completa concluída com sucesso!")


if __name__ == "__main__":
    run_full_cleanup()
