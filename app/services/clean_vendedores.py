import pandas as pd
import asyncio
from typing import List

from app.schemas.data_schemas import (
    VendedorSchema,
    VendedorLimpoSchema
)


def clean_vendedores(raw_data: List[VendedorSchema]) -> List[VendedorLimpoSchema]:
    """
    Limpeza simples da planilha de Vendedores.
    Entrada: List[VendedorSchema]
    Saída:   List[VendedorLimpoSchema]
    """

    if not raw_data:
        return []

    # Converte para DataFrame
    df = pd.DataFrame([r.model_dump() for r in raw_data])

    # -------------------------
    # 1) Padronização de textos
    # -------------------------
    df["seller_city"] = (
        df["seller_city"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    df["seller_state"] = (
        df["seller_state"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    # -------------------------
    # 2) Campos numéricos
    # -------------------------
    df["seller_zip_code_prefix"] = pd.to_numeric(
        df["seller_zip_code_prefix"], errors="coerce"
    )

    # Preenchimento opcional (mediana)
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].fillna(
        df["seller_zip_code_prefix"].median()
    )

    # -------------------------
    # 3) Retornar nos Schemas
    # -------------------------
    cleaned_records = df.to_dict("records")
    return [VendedorLimpoSchema(**record) for record in cleaned_records]


async def clean_vendedores_async(
    records: List[VendedorSchema],
) -> List[VendedorLimpoSchema]:
    return await asyncio.to_thread(clean_vendedores, records)
