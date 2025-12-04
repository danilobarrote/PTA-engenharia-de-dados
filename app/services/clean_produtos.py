import pandas as pd
from ..schemas.data_schemas import ProdutoSchema, ProdutosLimpoSchema

def clean_single_produto(data: ProdutoSchema) -> ProdutosLimpoSchema:
    # Limpeza de texto
    cat = data.product_category_name
    if not cat:
        cat = "indefinido"
    else:
        cat = cat.lower().replace(" ", "_")
    
    return ProdutosLimpoSchema(
        **data.model_dump(exclude={'product_category_name'}),
        product_category_name=cat
    )

def clean_produtos_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Texto
    df['product_category_name'] = df['product_category_name'].fillna('indefinido')
    df['product_category_name'] = df['product_category_name'].astype(str).str.lower().str.replace(' ', '_')

    # Numéricos e Mediana
    numeric_cols = [
        'product_name_lenght', 'product_description_lenght', 'product_photos_qty',
        'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'
    ]
    
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)

    return df