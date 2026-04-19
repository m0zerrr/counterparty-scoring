import pandas as pd
import numpy as np

def replace_empty_to_nan(val):
  if pd.isna(val):
    return np.nan
  
  if isinstance(val, str):
    val_stripped = val.strip()
    if val_stripped in ['', '[]', '{}', 'nan', 'None', 'null', 'NULL']:
      return np.nan

    if len(val_stripped) == 0:
      return np.nan

  return val

df = pd.read_csv('data/companies_data.csv')
print(f'Размер исходных данных: {df.shape}')

for col in df.columns:
  df[col] = df[col].apply(replace_empty_to_nan)

# Бинарные признаки по наличию данных
json_columns = ['all_cases_list', 'finance_revenue_by_year', 'finance_net_profit_by_year', 'taxes_list', 'fssp_proceedings']
for col in df.columns:
  if col in json_columns:
    df[f'has_{col}_data'] = df[col].notna().astype(int)

# Удаление пустых столбцов
pop_columns = [col for col in df.columns if df[col].isna().sum()/len(df) > 0.99]
df.drop(pop_columns, axis=1, inplace=True)


# Удаление технических ошибок
technical_error = df['status'].str.contains('признана недействительной|ошибочной')
df = df[~technical_error].copy()

print(f'Размер очищенных данных: {df.shape}')

