import pandas as pd
import numpy as np
import asyncio
from typing import List

from app.schemas.data_schemas import PedidoSchema, PedidoLimpoSchema


def clean_pedidos(raw_data: List[PedidoSchema]) -> List[PedidoLimpoSchema]:
    """
    Limpeza da planilha de Pedidos.
    - Padroniza colunas de data/hora
    - Corrige inconsistências de datas (entrega, envio, aprovação)
    - Padroniza o campo order_status
    - Cria colunas derivadas:
        * tempo_entrega_dias
        * tempo_entrega_estimado_dias
        * diferenca_entrega_dias
        * entrega_no_prazo
    - Garante que NaN/NaT sejam convertidos em None para os Schemas Pydantic.
    """

    if not raw_data:
        return []

    # -------------------------
    # 1) Converter lista de Schemas -> DataFrame
    # -------------------------
    df = pd.DataFrame([r.model_dump() for r in raw_data])

    # -------------------------
    # 2) Datas e Timestamp
    # -------------------------
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

    # -------------------------
    # 3) Tratamento de inconsistências de datas
    # -------------------------

    # 3.1) Pedidos delivered sem data de entrega do cliente
    filtro_delivered_sem_data = (
        (df["order_status"] == "delivered")
        & (df["order_delivered_customer_date"].isna())
    )

    if filtro_delivered_sem_data.any():
        tempo_entrega = (
            df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
        ).dt.days
        mediana_tempo_entrega = tempo_entrega.dropna().median()

        if not pd.isna(mediana_tempo_entrega):
            delta_mediana = pd.to_timedelta(mediana_tempo_entrega, unit="D")
            df.loc[filtro_delivered_sem_data, "order_delivered_customer_date"] = (
                df.loc[filtro_delivered_sem_data, "order_purchase_timestamp"]
                + delta_mediana
            )

    # 3.2) Pedidos shipped/delivered sem data de envio ao carrier
    filtro_envio_inconsistente = (
        df["order_delivered_carrier_date"].isna()
        & df["order_status"].isin(["shipped", "delivered"])
    )
    if filtro_envio_inconsistente.any():
        df.loc[filtro_envio_inconsistente, "order_delivered_carrier_date"] = df.loc[
            filtro_envio_inconsistente, "order_approved_at"
        ]

    # 3.3) Pedidos sem data de aprovação, mas não criados/cancelados
    filtro_aprovacao_inconsistente = (
        df["order_approved_at"].isna()
        & ~df["order_status"].isin(["created", "canceled"])
    )
    if filtro_aprovacao_inconsistente.any():
        df.loc[filtro_aprovacao_inconsistente, "order_approved_at"] = df.loc[
            filtro_aprovacao_inconsistente, "order_purchase_timestamp"
        ]

    # -------------------------
    # 4) Padronização de texto em order_status
    # -------------------------
    if "order_status" in df.columns:
        df["order_status"] = (
            df["order_status"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        order_status_pt = {
            "delivered": "entregue",
            "invoiced": "faturado",
            "shipped": "enviado",
            "processing": "em_processamento",
            "unavailable": "indisponivel",
            "canceled": "cancelado",
            "created": "criado",
            "approved": "aprovado",
        }

        df["order_status"] = df["order_status"].map(order_status_pt).fillna(
            "status_desconhecido"
        )

    # -------------------------
    # 5) Colunas derivadas
    # -------------------------

    # tempo_entrega_dias
    df["tempo_entrega_dias"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    # tempo_entrega_estimado_dias
    df["tempo_entrega_estimado_dias"] = (
        df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]
    ).dt.days

    # diferenca_entrega_dias
    df["diferenca_entrega_dias"] = (
        df["tempo_entrega_dias"] - df["tempo_entrega_estimado_dias"]
    )

    # entrega_no_prazo
    condicoes = [
        df["diferenca_entrega_dias"].isna(),       # não tem entrega
        df["diferenca_entrega_dias"] <= 0,         # entregou no prazo ou antes
        df["diferenca_entrega_dias"] > 0,          # entregou depois
    ]
    valores = [
        "Não Entregue",
        "Sim",
        "Não",
    ]

    df["entrega_no_prazo"] = np.select(condicoes, valores, default="Não Entregue")

    # -------------------------
    # 6) Garantir tipos numéricos e trocar NaN por None
    # -------------------------
    numeric_cols = [
        "tempo_entrega_dias",
        "tempo_entrega_estimado_dias",
        "diferenca_entrega_dias",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Trocar NaN/NaT por None para o Pydantic aceitar
    df = df.where(pd.notnull(df), None)

    cleaned_records = df.to_dict("records")

    # Garantir que qualquer float NaN restante nessas colunas vire None
    for rec in cleaned_records:
        for col in numeric_cols:
            v = rec.get(col)
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                rec[col] = None

    # -------------------------
    # 7) Retorno nos Schemas
    # -------------------------
    return [PedidoLimpoSchema(**record) for record in cleaned_records]


async def clean_pedidos_async(
    records: List[PedidoSchema],
) -> List[PedidoLimpoSchema]:
    """Wrapper assíncrono para rodar limpeza de pedidos em thread separada."""
    return await asyncio.to_thread(clean_pedidos, records)