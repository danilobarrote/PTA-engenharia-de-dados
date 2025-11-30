from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from pydantic import field_validator

# ==========================
# SCHEMAS BRUTOS (ENTRADA)
# ==========================

class PedidoSchema(BaseModel):
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: Optional[datetime] = None 
    order_approved_at: Optional[datetime] = None
    order_delivered_carrier_date: Optional[datetime] = None
    order_delivered_customer_date: Optional[datetime] = None
    order_estimated_delivery_date: Optional[datetime] = None


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
    seller_zip_code_prefix: int | str | None
    seller_city: str | int | None
    seller_state: str

    @field_validator("seller_city", mode="before")
    def force_city_to_string(cls, v):
        if v is None:
            return None
        return str(v)


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

class PedidoLimpoSchema(BaseModel):
    order_id: str
    customer_id: str
    order_status: str
    
    # DATETIME REAL
    order_purchase_timestamp: Optional[datetime] = None
    order_approved_at: Optional[datetime] = None
    order_delivered_carrier_date: Optional[datetime] = None
    order_delivered_customer_date: Optional[datetime] = None
    order_estimated_delivery_date: Optional[datetime] = None

    tempo_entrega_dias: Optional[int] = None
    tempo_entrega_estimado_dias: Optional[int] = None
    diferenca_entrega_dias: Optional[float] = None
    entrega_no_prazo: Optional[str] = None


class ProdutosLimpoSchema(ProdutoSchema):
    pass


class VendedorLimpoSchema(VendedorSchema):
    pass


class ItemPedidoLimpoSchema(ItemPedidoSchema):
    pass