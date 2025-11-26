import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
import asyncio
from app.schemas.data_schemas import PedidoSchema, PedidoLimpoSchema

def clean_pedidos(raw_data: List[PedidoSchema]) -> List[PedidoLimpoSchema]:

    data_as_dicts = [record.model_dump() for record in raw_data]

    df_pedidos = pd.DataFrame(data_as_dicts)

    """
    Datas e Timestamp
    """

    # Padronizando colunas de data e hora

    colunas_datas = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]

    for coluna in colunas_datas:

        df_pedidos[coluna] = pd.to_datetime(df_pedidos[coluna], format="%Y-%m-%d %H:%M:%S")
        df_pedidos[coluna] = df_pedidos[coluna].dt.strftime("%d/%m/%Y %H:%M:%S")

        # Formatando novamente para "datetime" para cálculos futuros
        df_pedidos[coluna] = pd.to_datetime(
                df_pedidos[coluna],
                format="%d/%m/%Y %H:%M:%S",
                errors='coerce'
        )
    

    """
    Tratamento de Dados Nulos
    """

    # Pedidos entregues e sem data de entrega

    # Adicionar a mediana na data de compra
    mediana_tempo_entrega = ((df_pedidos["order_delivered_customer_date"] - df_pedidos["order_purchase_timestamp"]).dt.days).median()
    delta_mediana = pd.to_timedelta(mediana_tempo_entrega, unit='D')

    filtro_entrega_inconsistente = (df_pedidos['order_status'] == 'delivered') & (df_pedidos['order_delivered_customer_date'].isnull())

    df_pedidos.loc[filtro_entrega_inconsistente, 'order_delivered_customer_date'] = \
        df_pedidos.loc[filtro_entrega_inconsistente, 'order_purchase_timestamp'] + delta_mediana


    # Pedidos entregues/enviados e sem data de envio

    # 1. Identificar os 2 pedidos com inconsistência de envio
    filtro_envio_inconsistente = (df_pedidos['order_delivered_carrier_date'].isnull()) & \
                                (df_pedidos['order_status'].isin(['shipped', 'delivered']))

    # 2. Imputar a data de envio com a data de aprovação (data anterior válida conhecida)
    df_pedidos.loc[filtro_envio_inconsistente, 'order_delivered_carrier_date'] = \
        df_pedidos.loc[filtro_envio_inconsistente, 'order_approved_at']
    

    # Pedidos entreges/enviados e sem data de aprovação

    # 1. Identificar os 14 pedidos com inconsistência de aprovação
    filtro_aprovacao_inconsistente = (df_pedidos['order_approved_at'].isnull()) & \
                                    (~df_pedidos['order_status'].isin(['created', 'canceled']))

    # 2. Imputar a data de aprovação com o timestamp da compra (data anterior válida conhecida)
    df_pedidos.loc[filtro_aprovacao_inconsistente, 'order_approved_at'] = \
        df_pedidos.loc[filtro_aprovacao_inconsistente, 'order_purchase_timestamp']


    """
    Padronização de Texto
    """

    order_status_pt = {
        "delivered": "entregue",
        "invoiced": "faturado",
        "shipped": "enviado",
        "processing": "em processamento",
        "unavailable": "indisponível",
        "canceled": "cancelado",
        "created": "criado",
        "approved": "aprovado"
    }

    # Garantir que todos os dados estejam corretos
    df_pedidos["order_status"] = (
        df_pedidos["order_status"]
        .astype("string")
        .str.lower()
        .str.strip()
        .map(order_status_pt)
    )

    # Se algum status não estiver no dicionário, evita ficar como NaN
    df_pedidos["order_status"] = df_pedidos["order_status"].fillna("status_desconhecido")


    """
    Criação colunas derivadas
    """

    # tempo_entrega_dias (Diferença data de entrega e data de compra)
    df_pedidos["tempo_entrega_dias"] = (df_pedidos["order_delivered_customer_date"] - df_pedidos["order_purchase_timestamp"]).dt.days

    # tempo_entrega_estimado_dias (Diferença da data estimada de entrega e data de compra)
    df_pedidos["tempo_entrega_estimado_dias"] = (df_pedidos["order_estimated_delivery_date"] - df_pedidos["order_purchase_timestamp"]).dt.days

    # diferenca_entrega_dias (Diferença entre as duas colunas anteriores)
    df_pedidos["diferenca_entrega_dias"] = df_pedidos["tempo_entrega_dias"] - df_pedidos["tempo_entrega_estimado_dias"]

    # entrega_no_prazo (Indicador se a entrega ocorreu no prazo, se foi fora do prazo ou não ocorreu)
    entrega_prazo = [
        df_pedidos['diferenca_entrega_dias'].isna(),
        df_pedidos['diferenca_entrega_dias'] <= 0,
        df_pedidos['diferenca_entrega_dias'] > 0
    ]

    status_entrega = [
        "Não Entregue",
        "Sim",
        "Não"
    ]

    df_pedidos["entrega_no_prazo"] = np.select(
        entrega_prazo,
        status_entrega,
        default=""
    )
    
    cleaned_records = df_pedidos.to_dict('records')
    return [PedidoLimpoSchema(**record) for record in cleaned_records]

async def clean_pedidos_async(records: List[PedidoSchema]) -> List[PedidoLimpoSchema]:
    return await asyncio.to_thread(clean_pedidos, records)