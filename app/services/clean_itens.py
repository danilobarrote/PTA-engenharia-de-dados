import pandas as pd
import asyncio
from typing import List

from app.schemas.data_schemas import (
    ItemPedidoSchema,
    ItemPedidoLimpoSchema,
)


def clean_itens(raw_data: List[ItemPedidoSchema]) -> List[ItemPedidoLimpoSchema]:
    """
    Limpeza da planilha de Itens de Pedidos.
    - Converte price e freight_value para float e preenche nulos com a mediana.
    - Converte shipping_limit_date para datetime.
    Entrada: List[ItemPedidoSchema]
    Saída:   List[ItemPedidoLimpoSchema]
    """

    if not raw_data:
        return []

    # Converte lista de Pydantic -> DataFrame
    df = pd.DataFrame([r.model_dump() for r in raw_data])

    # -------------------------
    # 1) Colunas numéricas
    # -------------------------
    for col in ["price", "freight_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)

    # -------------------------
    # 2) Datas
    # -------------------------
    if "shipping_limit_date" in df.columns:
        df["shipping_limit_date"] = pd.to_datetime(
            df["shipping_limit_date"],
            errors="coerce",
        )

    # -------------------------
    # 3) Retorno nos Schemas
    # -------------------------
    cleaned_records = df.to_dict("records")
    return [ItemPedidoLimpoSchema(**record) for record in cleaned_records]


async def clean_itens_async(
    records: List[ItemPedidoSchema],
) -> List[ItemPedidoLimpoSchema]:
    """Wrapper assíncrono para rodar o pandas em thread separada."""
    return await asyncio.to_thread(clean_itens, records)
