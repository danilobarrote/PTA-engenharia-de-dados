from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


# ==========================
# SCHEMAS BRUTOS (ENTRADA)
# ==========================

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


# ==========================
# SCHEMAS LIMPOS (SAÍDA)
# ==========================

class PedidoLimpoSchema(PedidoSchema):
    tempo_entrega_dias: Optional[int]
    tempo_entrega_estimado_dias: Optional[int]
    diferenca_entrega_dias: Optional[float]
    entrega_no_prazo: str


class ProdutosLimpoSchema(ProdutoSchema):
    pass


class VendedorLimpoSchema(VendedorSchema):
    pass


class ItemPedidoLimpoSchema(ItemPedidoSchema):
    pass


# ==========================
# ENVELOPE DE ENTRADA
# ==========================

class SchemaRecepcaoDatasets(BaseModel):
    dataset1_vendedores: List[VendedorSchema] = Field(
        ..., description="Lista de Vendedores."
    )
    dataset2_clientes: List[ProdutoSchema] = Field(
        ..., description="Lista de Produtos."
    )
    dataset3_itens: List[ItemPedidoSchema] = Field(
        ..., description="Lista de Itens de Pedidos."
    )
    dataset4_pedidos: List[PedidoSchema] = Field(
        ..., description="Lista de Pedidos."
    )


# ==========================
# ENVELOPE DE SAÍDA
# ==========================

class AllDatasetsLimpos(BaseModel):
    vendedores: List[VendedorLimpoSchema]
    produtos: List[ProdutosLimpoSchema]
    transacoes: List[ItemPedidoLimpoSchema]
    # aqui ainda mandamos pedidos brutos (schema de entrada)
    pedidos: List[PedidoSchema]