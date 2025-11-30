import pandas as pd
import numpy as np
import asyncio
from typing import List
from datetime import datetime

from app.schemas.data_schemas import PedidoSchema, PedidoLimpoSchema


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

    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: x.item() if hasattr(x, "item") else x
            if x is not None
            else None
        )

    return df


def clean_pedidos(raw_data: List[PedidoSchema]) -> List[PedidoLimpoSchema]:

    if not raw_data:
        return []

    df = pd.DataFrame([r.model_dump() for r in raw_data])

    # Datas
    colunas_datas = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for coluna in colunas_datas:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce")

    # Correções de datas
    filtro_delivered_sem_data = (
        (df["order_status"] == "delivered")
        & (df["order_delivered_customer_date"].isna())
    )

    if filtro_delivered_sem_data.any():
        tempo_entrega = (
            df["order_delivered_customer_date"]
            - df["order_purchase_timestamp"]
        ).dt.days
        mediana = tempo_entrega.dropna().median()

        if not pd.isna(mediana):
            delta = pd.to_timedelta(mediana, unit="D")
            df.loc[filtro_delivered_sem_data, "order_delivered_customer_date"] = (
                df.loc[filtro_delivered_sem_data, "order_purchase_timestamp"] + delta
            )

    filtro_envio = (
        df["order_delivered_carrier_date"].isna()
        & df["order_status"].isin(["shipped", "delivered"])
    )
    if filtro_envio.any():
        df.loc[filtro_envio, "order_delivered_carrier_date"] = df.loc[
            filtro_envio, "order_approved_at"
        ]

    filtro_aprov = (
        df["order_approved_at"].isna()
        & ~df["order_status"].isin(["created", "canceled"])
    )
    if filtro_aprov.any():
        df.loc[filtro_aprov, "order_approved_at"] = df.loc[
            filtro_aprov, "order_purchase_timestamp"
        ]

    # Status
    df["order_status"] = (
        df["order_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    mapper = {
        "delivered": "entregue",
        "invoiced": "faturado",
        "shipped": "enviado",
        "processing": "em_processamento",
        "unavailable": "indisponivel",
        "canceled": "cancelado",
        "created": "criado",
        "approved": "aprovado",
    }
    df["order_status"] = df["order_status"].replace(mapper)

    validos = set(mapper.values())
    df.loc[~df["order_status"].isin(validos), "order_status"] = "status_desconhecido"

    # Colunas derivadas
    df["tempo_entrega_dias"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    df["tempo_entrega_estimado_dias"] = (
        df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]
    ).dt.days

    df["diferenca_entrega_dias"] = (
        df["tempo_entrega_dias"] - df["tempo_entrega_estimado_dias"]
    )

    cond = [
        df["diferenca_entrega_dias"].isna(),
        df["diferenca_entrega_dias"] <= 0,
        df["diferenca_entrega_dias"] > 0,
    ]
    vals = ["Não Entregue", "Sim", "Não"]
    df["entrega_no_prazo"] = np.select(cond, vals, default="Não Entregue")

    numeric_cols = [
        "tempo_entrega_dias",
        "tempo_entrega_estimado_dias",
        "diferenca_entrega_dias",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalização final
    df = normalize_df_for_pydantic(df, colunas_datas)

    if "tempo_entrega_dias" in df.columns:
        df["tempo_entrega_dias"] = df["tempo_entrega_dias"].apply(
            lambda x: None if x is None or (isinstance(x, float) and pd.isna(x)) else int(x)
        )

    cleaned_records = df.to_dict("records")

    for rec in cleaned_records:
        for key, value in rec.items():
            # pandas NaT vira float('nan')
            if isinstance(value, float) and pd.isna(value):
                rec[key] = None

            # pandas.Timestamp -> datetime
            if isinstance(value, pd.Timestamp):
                rec[key] = value.to_pydatetime()
                
    return [PedidoLimpoSchema(**r) for r in cleaned_records]


async def clean_pedidos_async(records: List[PedidoSchema]) -> List[PedidoLimpoSchema]:
    return await asyncio.to_thread(clean_pedidos, records)
