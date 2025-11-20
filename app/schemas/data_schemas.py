from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class PedidoSchema(BaseModel):
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: Optional[datetime]
    order_delivered_carrier_date: Optional[datetime]
    order_delivered_customer_date: Optional[datetime]
    order_estimated_delivery_date: datetime

class ProdutoSchema(BaseModel):
    product_id: str
    product_category_name: Optional[str]
    product_name_lenght: Optional[float]
    product_description_lenght: Optional[float]
    product_photos_qty: Optional[float]
    product_weight_g: Optional[float]
    product_length_cm: Optional[float]
    product_height_cm: Optional[float]
    product_width_cm: Optional[float]

class VendedorSchema(BaseModel):
    seller_id: str
    seller_zip_code_prefix: int
    seller_city: str
    seller_state: str

class ItemPedidoSchema(BaseModel):
    order_id: str
    order_item_id: int
    product_id: str
    seller_id: str
    shipping_limit_date: datetime
    price: Optional[float]
    freight_value: Optional[float]

class SchemaRecepcaoDatasets(BaseModel):
    dataset1_vendedores: List[VendedorSchema]
    dataset2_clientes: List[ProdutoSchema]
    dataset3_transacoes: List[ItemPedidoSchema]
    dataset4_pedidos: List[PedidoSchema]

class RespostaStatusProcessamento(BaseModel):
    status: str = "Recebido"
    message: str = "Dados recebidos com sucesso. Processamento inicial (mock) concluído."
    datasets_received: int = 4
    total_records_processed: int