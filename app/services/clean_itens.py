import pandas as pd
from schemas.data_schemas import ItemPedidoSchema, ItemPedidoLimpoSchema

def clean_single_item(data: ItemPedidoSchema) -> ItemPedidoLimpoSchema:
    return ItemPedidoLimpoSchema(**data.model_dump())

def clean_itens_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    cols_float = ['price', 'freight_value']
    for col in cols_float:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)
    
    # Datas
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    
    return df