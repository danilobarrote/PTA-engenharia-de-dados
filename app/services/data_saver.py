import pandas as pd
import gspread

def save_df_to_sheet(sheet_object: gspread.Worksheet, df: pd.DataFrame):
    # Substituir NaT/NaN por strings vazias ou None para o JSON do Google Sheets aceitar
    df_clean = df.astype(object).where(pd.notnull(df), None)
    
    # Converter datas para string para evitar erro de serialização
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df_clean[col] = df_clean[col].astype(str).replace('NaT', '')

    data = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
    
    # Limpa e reescreve
    sheet_object.clear()
    sheet_object.update(data)