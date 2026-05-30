import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings

# Настройка библиотек
warnings.simplefilter('ignore')
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = [12, 6]
plt.rcParams['font.size'] = 13

INPUT_CSV = 'data/intermediate/companies_ready.csv'
OUTPUT_CSV = 'data/intermediate/companies_encoded.csv'


def main():
  df = pd.read_csv(INPUT_CSV, low_memory=False)
  print(f'Размер исходного набора данных: {df.shape}')
  print(df.info())
  print(df.head())
  print(f'Количество пропущенных значений:\n{df.isna().sum()[df.isna().sum() > 0]}')
  
  status_counts = df['status_egr_encoded'].value_counts().sort_index()
  status_labels = {
    0: 'Действующее',
    1: 'В процессе ликв./банк.',
    2: 'Реорг.',
    3: 'Ликвидировано/Банкрот',
    -1: 'Неизвестно',
  }
  
  plt.figure(figsize=(10,5))
  bars = plt.bar([status_labels.get(i, str(i)) for i in status_counts.index], status_counts.values, color='skyblue')
  plt.title('Распределение компаний по статусу ЕГРЮЛ')
  plt.ylabel('Количество компаний')
  plt.xticks(rotation=45)
  plt.tight_layout()
  plt.show()

if __name__ == '__main__':
  main()