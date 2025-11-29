import pandas as pd
import asyncio
from typing import List

from app.schemas.data_schemas import (
    ProdutoSchema,
    ProdutosLimpoSchema,
)

def clean_produtos(raw_data: List[ProdutoSchema]) -> List[ProdutosLimpoSchema]:
    """
    Limpeza da planilha de Produtos.
    - Normaliza colunas numéricas (conversão para float + mediana)
    - Mantém category_name como string/None
    - Nenhuma regra de enriquecimento adicional é necessária
    Entrada: List[ProdutoSchema]
    Saída:   List[ProdutosLimpoSchema]
    """

    if not raw_data:
        return []

    # Converte de Pydantic → DataFrame
    df = pd.DataFrame([r.model_dump() for r in raw_data])

    # -------------------------
    # 1) Colunas numéricas
    # -------------------------
    colunas_numericas = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)

    # -------------------------
    # 2) Padronização (texto)
    # -------------------------
    if "product_category_name" in df.columns:
        df["product_category_name"] = (
            df["product_category_name"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace("none", pd.NA)
        )

    # -------------------------
    # 3) Retorno nos Schemas
    # -------------------------
    cleaned_records = df.to_dict("records")
    return [ProdutosLimpoSchema(**record) for record in cleaned_records]


async def clean_produtos_async(
    records: List[ProdutoSchema],
) -> List[ProdutosLimpoSchema]:
    """Wrapper assíncrono para rodar o pandas em background."""
    return await asyncio.to_thread(clean_produtos, records)
