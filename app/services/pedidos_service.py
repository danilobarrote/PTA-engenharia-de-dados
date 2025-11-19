import pandas as pd
import numpy as np
from typing import List, Dict, Any
from app.schemas.example import ExampleData, BaseModel

def clean_pedidos(raw_data: List[ExampleData]) -> List[BaseModel]:

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
    Padronização de Texto
    """

    order_status_pt = {
        "delivered": "entregue",
        "invoiced": "faturado",
        "shipped": "strftienviado",
        "processing": "em processamento",
        "unavailable": "indisponível",
        "canceled": "cancelado",
        "created": "criado",
        "approved": "aprovado"
    }

    df_pedidos["order_status"] = df_pedidos["order_status"].map(order_status_pt)

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
    return [BaseModel(**record) for record in cleaned_records]