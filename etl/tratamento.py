import pandas as pd
import unicodedata


# ----------------------------------------
# Função auxiliar: remover acentos
# ----------------------------------------
def remover_acentos(texto: str):
    """
    Remove acentos de um texto e devolve o valor limpo.
    Se o valor for nulo (NaN), retorna como está.
    """
    if pd.isna(texto):
        return texto

    texto_convertido = str(texto)
    texto_normalizado = unicodedata.normalize("NFKD", texto_convertido)

    return "".join(
        c for c in texto_normalizado
        if not unicodedata.combining(c)
    )


# ----------------------------------------
# 1. TRATAMENTO DE VENDEDORES
# ----------------------------------------
def tratar_sellers(tabela_vendedores: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza cidades e estados dos vendedores:
    - seller_city: remove acentos, UPPERCASE, strip
    - seller_state: UPPERCASE, strip
    """
    tabela = tabela_vendedores.copy()

    tabela["seller_city"] = (
        tabela["seller_city"]
        .apply(remover_acentos)
        .str.upper()
        .str.strip()
    )

    tabela["seller_state"] = (
        tabela["seller_state"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    return tabela


# ----------------------------------------
# 2. TRATAMENTO DE PRODUTOS
# ----------------------------------------
def tratar_produtos(tabela_produtos: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta dados de produtos:
    - product_category_name em lowercase, com espaços substituídos por '_'
      e nulos preenchidos com a MODA da própria coluna (ou 'indefinido' se não houver moda).
    - Colunas numéricas têm valores nulos preenchidos pela MEDIANA.
    """
    tabela = tabela_produtos.copy()

    # -------- Tratamento da coluna de categoria --------
    if "product_category_name" in tabela.columns:
        col = (
            tabela["product_category_name"]
            .astype("string")
            .str.lower()
            .str.strip()
            .str.replace(" ", "_", regex=False)
        )

        # Moda considerando apenas valores não nulos
        moda = col.dropna().mode()
        valor_moda = moda.iloc[0] if not moda.empty else "indefinido"

        # Preenche valores nulos com a moda
        col = col.fillna(valor_moda)

        tabela["product_category_name"] = col

    # -------- Preenchimento de nulos numéricos com MEDIANA --------
    colunas_numericas = tabela.select_dtypes(include="number").columns

    for coluna in colunas_numericas:
        mediana = tabela[coluna].median()
        tabela[coluna] = tabela[coluna].fillna(mediana)

    return tabela


# ----------------------------------------
# 3. TRATAMENTO DE ITENS DE PEDIDOS
# ----------------------------------------
def tratar_itens_pedidos(tabela_itens: pd.DataFrame) -> pd.DataFrame:
    """
    Trata itens de pedidos:
    - price e freight_value: converte para numérico e preenche nulos com a MEDIANA.
    - shipping_limit_date: converte para datetime e preenche datas inválidas (NaT) com a MODA.
    """
    tabela = tabela_itens.copy()

    # -------- price e freight_value (MEDIANA) --------
    for col in ["price", "freight_value"]:
        if col in tabela.columns:
            tabela[col] = pd.to_numeric(tabela[col], errors="coerce")
            mediana = tabela[col].median()
            tabela[col] = tabela[col].fillna(mediana)

    # -------- shipping_limit_date (MODA) --------
    if "shipping_limit_date" in tabela.columns:
        tabela["shipping_limit_date"] = pd.to_datetime(
            tabela["shipping_limit_date"],
            errors="coerce",
        )

        moda_datas = tabela["shipping_limit_date"].dropna().mode()
        if not moda_datas.empty:
            valor_moda_data = moda_datas.iloc[0]
            tabela["shipping_limit_date"] = tabela["shipping_limit_date"].fillna(valor_moda_data)

    return tabela


# ----------------------------------------
# 4. AJUSTAR IDS ÓRFÃOS SUBSTITUINDO PELA MODA
# ----------------------------------------
def ajustar_ids_orfaos_com_moda(
    tabela_itens: pd.DataFrame,
    tabela_pedidos: pd.DataFrame,
    tabela_produtos: pd.DataFrame,
    tabela_vendedores: pd.DataFrame,
):
    """
    Ajusta itens 'órfãos' SEM remover linhas.

    Sempre que order_id, product_id ou seller_id forem:
      - nulos ou
      - não existirem nas tabelas de referência,

    o valor é SUBSTITUÍDO pela MODA da respectiva coluna
    nas tabelas de referência:

      - order_id   -> moda de tabela_pedidos["order_id"]
      - product_id -> moda de tabela_produtos["product_id"]
      - seller_id  -> moda de tabela_vendedores["seller_id"]
    """
    tabela = tabela_itens.copy()

    # ---------- IDs válidos ----------
    pedidos_validos = set(tabela_pedidos["order_id"].dropna().unique())
    produtos_validos = set(tabela_produtos["product_id"].dropna().unique())
    vendedores_validos = set(tabela_vendedores["seller_id"].dropna().unique())

    # ---------- Máscaras de órfãos/inválidos ----------
    mask_order = tabela["order_id"].isna() | ~tabela["order_id"].isin(pedidos_validos)
    mask_prod = tabela["product_id"].isna() | ~tabela["product_id"].isin(produtos_validos)
    mask_sell = tabela["seller_id"].isna() | ~tabela["seller_id"].isin(vendedores_validos)

    # ---------- Modas de REFERÊNCIA ----------
    moda_order = tabela_pedidos["order_id"].dropna().mode()
    moda_order_val = moda_order.iloc[0] if not moda_order.empty else None

    moda_prod = tabela_produtos["product_id"].dropna().mode()
    moda_prod_val = moda_prod.iloc[0] if not moda_prod.empty else None

    moda_sell = tabela_vendedores["seller_id"].dropna().mode()
    moda_sell_val = moda_sell.iloc[0] if not moda_sell.empty else None

    # ---------- SUBSTITUIÇÃO (sem remoção de linhas) ----------
    if moda_order_val is not None:
        tabela.loc[mask_order, "order_id"] = moda_order_val

    if moda_prod_val is not None:
        tabela.loc[mask_prod, "product_id"] = moda_prod_val

    if moda_sell_val is not None:
        tabela.loc[mask_sell, "seller_id"] = moda_sell_val

    resumo = {
        "linhas_totais": len(tabela),
        "itens_corrigidos_order_id": int(mask_order.sum()),
        "itens_corrigidos_product_id": int(mask_prod.sum()),
        "itens_corrigidos_seller_id": int(mask_sell.sum()),
        "order_id_moda_usado": moda_order_val,
        "product_id_moda_usado": moda_prod_val,
        "seller_id_moda_usado": moda_sell_val,
    }

    return tabela, resumo
