import pandas as pd
import unicodedata


def remover_acentos(texto: str):
    """
    Remove acentos de um texto e devolve o valor limpo.
    Caso o valor seja nulo, apenas retorna como está.
    """
    if pd.isna(texto):
        return texto

    texto = str(texto)
    texto_normalizado = unicodedata.normalize("NFKD", texto)

    texto_sem_acentos = "".join(
        caractere for caractere in texto_normalizado
        if not unicodedata.combining(caractere)
    )

    return texto_sem_acentos


def tratar_sellers(tabela_vendedores: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza cidades e estados dos vendedores,
    removendo acentos e colocando tudo em maiúsculas.
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


def tratar_produtos(tabela_produtos: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta dados de produtos padronizando categorias
    e preenchendo valores numéricos nulos com a mediana.
    """
    tabela = tabela_produtos.copy()

    if "product_category_name" in tabela.columns:
        # Padroniza a categoria antes de tratar nulos
        coluna_categoria = (
            tabela["product_category_name"]
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        # Identifica a moda (categoria mais frequente)
        modas = coluna_categoria.mode()
        if not modas.empty:
            valor_moda = modas.iloc[0]
        else:
            valor_moda = "indefinido"

        # Preenche valores ausentes com a moda
        coluna_categoria = coluna_categoria.fillna(valor_moda)

        tabela["product_category_name"] = coluna_categoria

    # Preenche valores nulos em colunas numéricas com a mediana
    colunas_numericas = tabela.select_dtypes(include="number").columns

    for coluna in colunas_numericas:
        mediana = tabela[coluna].median()
        tabela[coluna] = tabela[coluna].fillna(mediana)

    return tabela


def tratar_itens_pedidos(tabela_itens: pd.DataFrame) -> pd.DataFrame:
    """
    Trata itens de pedidos convertendo valores numéricos
    e ajustando datas para o formato correto.
    """
    tabela = tabela_itens.copy()

    for coluna in ["price", "freight_value"]:
        if coluna in tabela.columns:
            tabela[coluna] = pd.to_numeric(tabela[coluna], errors="coerce")
            mediana = tabela[coluna].median()
            tabela[coluna] = tabela[coluna].fillna(mediana)

    if "shipping_limit_date" in tabela.columns:
        tabela["shipping_limit_date"] = pd.to_datetime(
            tabela["shipping_limit_date"],
            errors="coerce"
        )

    return tabela


def remover_orfaos_itens(
    tabela_itens: pd.DataFrame,
    tabela_pedidos: pd.DataFrame,
    tabela_produtos: pd.DataFrame,
    tabela_vendedores: pd.DataFrame,
):
    """
    Remove registros de itens de pedidos que não têm
    correspondência nas tabelas de pedidos, produtos ou vendedores.
    """
    tabela = tabela_itens.copy()

    pedidos_validos = set(tabela_pedidos["order_id"].unique())
    produtos_validos = set(tabela_produtos["product_id"].unique())
    vendedores_validos = set(tabela_vendedores["seller_id"].unique())

    mask_order = tabela["order_id"].isin(pedidos_validos)
    mask_product = tabela["product_id"].isin(produtos_validos)
    mask_seller = tabela["seller_id"].isin(vendedores_validos)

    mask_final = mask_order & mask_product & mask_seller

    total_antes = len(tabela)
    total_depois = mask_final.sum()

    resumo = {
        "total_antes": total_antes,
        "total_depois": total_depois,
        "total_removidos": total_antes - total_depois,
        "removidos_por_order_id_invalido": int((~mask_order).sum()),
        "removidos_por_product_id_invalido": int((~mask_product).sum()),
        "removidos_por_seller_id_invalido": int((~mask_seller).sum()),
    }

    tabela_filtrada = tabela[mask_final].copy()

    return tabela_filtrada, resumo
