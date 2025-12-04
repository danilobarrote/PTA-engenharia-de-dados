from datetime import datetime, timedelta
import pandas as pd
from ..schemas.data_schemas import PedidoSchema, PedidoLimpoSchema

STATUS_TRANSLATION = {
    "delivered": "entregue",
    "invoiced": "faturado",
    "shipped": "enviado",
    "processing": "em processamento",
    "unavailable": "indisponível",
    "canceled": "cancelado",
    "created": "criado",
    "approved": "aprovado"
}

def clean_single_pedido(data: PedidoSchema) -> PedidoLimpoSchema:
    # 1. Tradução e Padronização do Status
    status_raw = data.order_status.lower() if data.order_status else ""
    new_status = STATUS_TRANSLATION.get(status_raw, status_raw)

    # 2. Extração das Datas (para variáveis locais mutáveis)
    purchase = data.order_purchase_timestamp
    approved = data.order_approved_at
    carrier = data.order_delivered_carrier_date
    delivered = data.order_delivered_customer_date
    estimated = data.order_estimated_delivery_date

    # =========================================================================
    # LÓGICA DE CONTEXTO (PREENCHIMENTO INTELIGENTE)
    # =========================================================================

    # 2.1. Se não tem DATA DE COMPRA, mas tem outras datas, inferimos a compra.
    # Lógica: A compra acontece antes da aprovação. Se não tivermos a data exata,
    # assumimos que a compra ocorreu no mesmo momento da aprovação (ou envio).
    if not purchase:
        if approved:
            purchase = approved
        elif carrier:
            purchase = carrier
        elif delivered:
            purchase = delivered
    
    # 2.2. Se não tem DATA DE APROVAÇÃO, mas o status indica progresso.
    # Se o pedido não está cancelado/criado, ele foi aprovado. 
    # Assumimos aprovação = data da compra.
    if not approved and new_status not in ['criado', 'cancelado', 'indisponível']:
        if purchase:
            approved = purchase

    # 2.3. Se não tem DATA DE ENVIO, mas status é 'enviado' ou 'entregue'.
    # Assumimos que foi enviado no momento da aprovação.
    if not carrier and new_status in ['enviado', 'entregue']:
        if approved:
            carrier = approved
        elif purchase:
            carrier = purchase

    # =========================================================================
    # 3. CÁLCULOS DE KPI (Usando as datas já corrigidas acima)
    # =========================================================================
    tempo_entrega = None
    tempo_estimado = None
    diferenca = None
    no_prazo = "Não Entregue"

    # Só conseguimos calcular métricas se tivermos ao menos a Data de Compra (agora preenchida)
    if purchase:
        # Cálculo de tempo estimado (Estimated - Purchase)
        if estimated:
            tempo_estimado = (estimated - purchase).days

        # Cálculo de entrega real (Delivered - Purchase)
        if delivered:
            tempo_entrega = (delivered - purchase).days
            
            # Cálculo de atraso (Delivered - Estimated)
            if estimated:
                diferenca = (delivered - estimated).days
                if diferenca <= 0:
                    no_prazo = "Sim"  # Entregue no prazo ou adiantado
                else:
                    no_prazo = "Não"  # Atrasado

    return PedidoLimpoSchema(
        **data.model_dump(exclude={
            'order_status', 
            'order_purchase_timestamp', 
            'order_approved_at', 
            'order_delivered_carrier_date'
        }), 
        order_status=new_status,
        
        # Retornamos as datas corrigidas/inferidas
        order_purchase_timestamp=purchase,
        order_approved_at=approved,
        order_delivered_carrier_date=carrier,
        
        # KPIs calculados
        tempo_entrega_dias=tempo_entrega,
        tempo_entrega_estimado_dias=tempo_estimado,
        diferenca_entrega_dias=diferenca,
        entrega_no_prazo=no_prazo
    )

def clean_pedidos_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Conversão para Datetime
    cols_data = ['order_purchase_timestamp', 'order_approved_at', 
                 'order_delivered_carrier_date', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    
    for col in cols_data:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 2. Tradução de Status
    df['order_status'] = df['order_status'].str.lower().map(STATUS_TRANSLATION).fillna(df['order_status'])

    # 3. Lógica de Preenchimento (Contexto em Lote)
    
    # Se purchase nulo, preenche com approved
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].fillna(df['order_approved_at'])
    # Se ainda nulo, preenche com carrier
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].fillna(df['order_delivered_carrier_date'])
    
    # Se approved nulo e status avançado, preenche com purchase
    status_avancados = ['aprovado', 'enviado', 'entregue', 'faturado', 'em processamento']
    mask_aprovacao = (df['order_approved_at'].isna()) & (df['order_status'].isin(status_avancados))
    df.loc[mask_aprovacao, 'order_approved_at'] = df.loc[mask_aprovacao, 'order_purchase_timestamp']

    # Se carrier nulo e status enviado/entregue, preenche com approved
    mask_envio = (df['order_delivered_carrier_date'].isna()) & (df['order_status'].isin(['enviado', 'entregue']))
    df.loc[mask_envio, 'order_delivered_carrier_date'] = df.loc[mask_envio, 'order_approved_at']

    # 4. Cálculos
    df['tempo_entrega_dias'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days
    df['tempo_entrega_estimado_dias'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.days
    df['diferenca_entrega_dias'] = (df['order_delivered_customer_date'] - df['order_estimated_delivery_date']).dt.days

    def check_prazo(row):
        if pd.isna(row['order_delivered_customer_date']):
            return "Não Entregue"
        if pd.isna(row['diferenca_entrega_dias']):
            return "Indefinido"
        return "Sim" if row['diferenca_entrega_dias'] <= 0 else "Não"

    df['entrega_no_prazo'] = df.apply(check_prazo, axis=1)
    
    return df