from pathlib import Path
import sys
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from etl.tratamento import (
    tratar_sellers,
    tratar_produtos,
    tratar_itens_pedidos,
    ajustar_ids_orfaos_com_moda,
)


# ---------- DEFINIÇÃO DE CAMINHOS ----------
base_dir = BASE_DIR 
print("BASE_DIR:", base_dir)

# Caminhos dos arquivos CSV brutos
caminho_vendedores = base_dir / "base" / "vendedores.csv"
caminho_produtos = base_dir / "base" / "produtos.csv"
caminho_itens_pedidos = base_dir / "base" / "itens_pedidos.csv"
caminho_pedidos = base_dir / "base" / "pedidos.csv"

print("\nArquivos encontrados:")
print("vendedores:", caminho_vendedores.exists())
print("produtos:", caminho_produtos.exists())
print("itens_pedidos:", caminho_itens_pedidos.exists())
print("pedidos:", caminho_pedidos.exists())

# ---------- LEITURA DOS CSVs BRUTOS ----------
tabela_vendedores = pd.read_csv(caminho_vendedores)
tabela_produtos = pd.read_csv(caminho_produtos)
tabela_itens_pedidos = pd.read_csv(caminho_itens_pedidos)
tabela_pedidos = pd.read_csv(caminho_pedidos)

print("\nVENDEDORES - ANTES DO TRATAMENTO:")
print(tabela_vendedores[["seller_city", "seller_state"]].head(10))

print("\nPRODUTOS - ANTES DO TRATAMENTO:")
print(tabela_produtos.head())
print(tabela_produtos.dtypes)

print("\nITENS DE PEDIDOS - ANTES DO TRATAMENTO:")
print(tabela_itens_pedidos[["price", "freight_value", "shipping_limit_date"]].head())
print(tabela_itens_pedidos[["price", "freight_value"]].dtypes)

print("\nPEDIDOS - VISÃO GERAL (BRUTO):")
print(tabela_pedidos.head())

# ---------- TRATAMENTO DE CADA TABELA ----------
tabela_vendedores_tratada = tratar_sellers(tabela_vendedores)
tabela_produtos_tratada = tratar_produtos(tabela_produtos)
tabela_itens_pedidos_tratada = tratar_itens_pedidos(tabela_itens_pedidos)

print("\nVENDEDORES - DEPOIS DO TRATAMENTO:")
print(tabela_vendedores_tratada[["seller_city", "seller_state"]].head(10))

print("\nPRODUTOS - DEPOIS DO TRATAMENTO (CATEGORIA):")
print(tabela_produtos_tratada[["product_category_name"]].head(15))

print("\nPRODUTOS - DEPOIS DO TRATAMENTO (TIPOS):")
print(tabela_produtos_tratada.dtypes)

print("\nITENS DE PEDIDOS - DEPOIS DO TRATAMENTO:")
print(tabela_itens_pedidos_tratada[["price", "freight_value", "shipping_limit_date"]].head())
print(tabela_itens_pedidos_tratada[["price", "freight_value", "shipping_limit_date"]].dtypes)

# ---------- AJUSTE DE IDS ÓRFÃOS (SUBSTITUIÇÃO PELA MODA) ----------
tabela_itens_ajustada_ids, resumo_ajustes_ids = ajustar_ids_orfaos_com_moda(
    tabela_itens_pedidos_tratada,
    tabela_pedidos,
    tabela_produtos_tratada,
    tabela_vendedores_tratada,
)

print("\nRESUMO DOS AJUSTES DE IDS ÓRFÃOS (SUBSTITUIÇÃO PELA MODA):")
for chave, valor in resumo_ajustes_ids.items():
    print(f"{chave}: {valor}")

print("\nITENS DEPOIS DO AJUSTE DE IDS ÓRFÃOS:")
print(tabela_itens_ajustada_ids.head())

# ---------- SALVAR SAÍDAS TRATADAS ----------
diretorio_output = base_dir / "output"
diretorio_output.mkdir(exist_ok=True)

tabela_vendedores_tratada.to_csv(diretorio_output / "vendedores_tratado.csv", index=False)
tabela_produtos_tratada.to_csv(diretorio_output / "produtos_tratado.csv", index=False)
tabela_itens_ajustada_ids.to_csv(diretorio_output / "itens_pedidos_tratado.csv", index=False)

print("\nArquivos tratados salvos na pasta 'output/'.")
