import pandas as pd
from unidecode import unidecode
from schemas.data_schemas import VendedorSchema, VendedorLimpoSchema

def clean_single_vendedor(data: VendedorSchema) -> VendedorLimpoSchema:
    city = str(data.seller_city) if data.seller_city else ""
    state = str(data.seller_state) if data.seller_state else ""

    # Remove acentos e Uppercase
    city_clean = unidecode(city).upper()
    state_clean = state.upper()

    return VendedorLimpoSchema(
        **data.model_dump(exclude={'seller_city', 'seller_state'}),
        seller_city=city_clean,
        seller_state=state_clean
    )

def clean_vendedores_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['seller_city'] = df['seller_city'].astype(str).apply(lambda x: unidecode(x).upper() if x else "")
    df['seller_state'] = df['seller_state'].astype(str).str.upper()
    return df