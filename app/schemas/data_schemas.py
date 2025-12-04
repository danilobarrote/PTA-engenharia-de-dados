from pydantic import BaseModel, field_validator, ConfigDict, Field
from typing import Optional, Union, List
from datetime import datetime

# ==========================
# SCHEMAS BRUTOS (ENTRADA)
# ==========================

class PedidoSchema(BaseModel):
    row_number: Optional[int] = None
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: Optional[datetime] = None 
    order_approved_at: Optional[datetime] = None
    order_delivered_carrier_date: Optional[datetime] = None
    order_delivered_customer_date: Optional[datetime] = None
    order_estimated_delivery_date: Optional[datetime] = None

    # --- NOVO VALIDADOR PARA CORRIGIR DATAS ---
    @field_validator(
        "order_purchase_timestamp", 
        "order_approved_at", 
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date",
        mode="before"
    )
    def parse_dates(cls, v):
        # Se vier a string "None", vazia "", "nan" ou "NaT", transforma em None real
        if isinstance(v, str) and v in ("None", "", "nan", "NaT"):
            return None
        return v
    # ------------------------------------------


class ProdutoSchema(BaseModel):
    row_number: Optional[int] = None
    product_id: str
    product_category_name: Optional[str] = None
    product_name_lenght: Optional[float] = None
    product_description_lenght: Optional[float] = None
    product_photos_qty: Optional[float] = None
    product_weight_g: Optional[float] = None
    product_length_cm: Optional[float] = None
    product_height_cm: Optional[float] = None
    product_width_cm: Optional[float] = None

class VendedorSchema(BaseModel):
    row_number: Optional[int] = None
    seller_id: str
    seller_zip_code_prefix: Union[int, str, None] = None
    seller_city: Union[str, int, None] = None
    seller_state: Optional[str] = None

    @field_validator("seller_city", mode="before")
    def force_city_to_string(cls, v):
        if v is None:
            return None
        return str(v)

class ItemPedidoSchema(BaseModel):
    row_number: Optional[int] = None
    order_id: str
    order_item_id: int
    product_id: str
    seller_id: str
    shipping_limit_date: Optional[datetime] = None
    price: Optional[float] = None
    freight_value: Optional[float] = None

    # --- VALIDADOR PARA DATA DE ITEM TAMBÉM ---
    @field_validator("shipping_limit_date", mode="before")
    def parse_date_item(cls, v):
        if isinstance(v, str) and v in ("None", "", "nan", "NaT"):
            return None
        return v

# ==========================
# SCHEMAS LIMPOS (SAÍDA)
# ==========================

class PedidoLimpoSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    row_number: Optional[int] = None

    order_id: str
    customer_id: str
    order_status: str
    
    order_purchase_timestamp: Optional[datetime] = None
    order_approved_at: Optional[datetime] = None
    order_delivered_carrier_date: Optional[datetime] = None
    order_delivered_customer_date: Optional[datetime] = None
    order_estimated_delivery_date: Optional[datetime] = None

    tempo_entrega_dias: Optional[float] = None
    tempo_entrega_estimado_dias: Optional[float] = None
    diferenca_entrega_dias: Optional[float] = None
    entrega_no_prazo: Optional[str] = None

class ProdutosLimpoSchema(ProdutoSchema):
    pass

class VendedorLimpoSchema(VendedorSchema):
    pass

class ItemPedidoLimpoSchema(ItemPedidoSchema):
    pass