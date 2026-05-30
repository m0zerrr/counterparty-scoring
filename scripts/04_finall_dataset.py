# 04_finalize_dataset.py
import pandas as pd
import numpy as np
import re
import json
import ast
import warnings
from datetime import datetime
warnings.simplefilter('ignore')

INPUT_CSV = 'data/intermediate/cleaned_data_with_text.csv' 
OUTPUT_CSV = 'data/intermediate/companies_ready.csv'

# --- 1. НАДЕЖНЫЙ МАППИНГ СТАТУСОВ (РЕШАЕТ ПРОБЛЕМУ С КОДОМ 3) ---
def map_status_egr(status_str):
    """
    Прямой маппинг длинных строк status_egr в коды риска.
    Приоритет: Банкротство > Ликвидация > Реорганизация > Действующее.
    """
    if pd.isna(status_str):
        return -1
    
    s = str(status_str).lower().strip()
    
    # ПРИОРИТЕТ 1: БАНКРОТСТВО (Код 3)
    # Ищем ключевые слова, которые однозначно указывают на банкротство
    if any(kw in s for kw in [
        'несостоятельност', 
        'банкрот', 
        'конкурсное производство', 
        'наблюдение', 
        'финансовое оздоровление', 
        'внешнее управление'
    ]):
        return 3
        
    # ПРИОРИТЕТ 2: ЛИКВИДАЦИЯ И ИСКЛЮЧЕНИЕ (Код 2)
    # Сюда попадают все закрытые компании, НЕ являющиеся банкротами
    if any(kw in s for kw in [
        'ликвидация', 
        'исключение', 
        'недействующего', 
        'недостоверности', 
        'прекращение деятельности'
    ]):
        # Важно: исключаем реорганизацию, так как она тоже содержит "прекращение"
        if 'реорганизация' not in s and 'присоединения' not in s and 'слияния' not in s:
            return 2
            
    # ПРИОРИТЕТ 3: РЕОРГАНИЗАЦИЯ И ПРОЦЕССЫ (Код 1)
    if any(kw in s for kw in [
        'реорганизация', 
        'присоединения', 
        'слияния', 
        'разделения', 
        'преобразования', 
        'предстоящем исключении', 
        'в стадии ликвидации', 
        'находится в процессе'
    ]):
        return 1
        
    # ПРИОРИТЕТ 4: ДЕЙСТВУЮЩЕЕ (Код 0)
    if 'действующее' in s or 'действует' in s:
        return 0
        
    # ОСТАЛЬНОЕ (-1)
    return -1

# --- 2. ПАРСИНГ ДАТ ---
MONTHS_RU = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

def parse_russian_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == '':
        return pd.NaT
    
    date_str = str(date_str).strip().lower()
    
    # Формат "2 октября 2002"
    for month_name, month_num in MONTHS_RU.items():
        if month_name in date_str:
            pattern = r'(\d{1,2})\s+' + month_name + r'\s+(\d{4})'
            match = re.search(pattern, date_str)
            if match:
                day, year = match.groups()
                try:
                    return pd.Timestamp(year=int(year), month=month_num, day=int(day))
                except:
                    return pd.NaT
    
    # Форматы типа 10/2/02
    try:
        for fmt in ['%m/%d/%y', '%d/%m/%Y', '%m/%d/%Y']:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
    except:
        pass
    
    return pd.NaT

def parse_status_full(status_str):
    """
    Возвращает исходную строку статуса (без английских переводов) и дату.
    """
    if pd.isna(status_str) or str(status_str).strip() == '':
        return 'unknown', pd.NaT
    
    s = str(status_str).strip()
    status_date = pd.NaT
    
    if ',' in s:
        parts = s.split(',', 1)
        date_str = parts[1].strip()
        status_date = parse_russian_date(date_str)
        
    # Возвращаем исходный тип статуса (нижний регистр для единообразия, но не переводим)
    return s.lower(), status_date

def calc_lifetime(reg_date, status_date, status_code):
    if pd.isna(reg_date):
        return np.nan, np.nan
    
    if pd.notna(status_date):
        end_date = status_date
    elif status_code in [2, 3]: # Если ликвидировано или банкрот, а даты нет — берем сегодня
        end_date = pd.Timestamp.today()
    else:
        end_date = pd.Timestamp.today()
    
    lifetime_days = max(0, (end_date - reg_date).days)
    lifetime_years = round(lifetime_days / 365.25, 2)
    
    return lifetime_days, lifetime_years

def safe_json_load(x):
    if pd.isna(x) or x == "" or str(x).strip() in ['[]', '{}', 'nan', 'None']: 
        return None
    s = str(x).strip()
    
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, (list, dict)):
            return parsed
    except:
        pass
        
    try:
        parsed = json.loads(s.replace("'", '"'))
        if isinstance(parsed, (list, dict)):
            return parsed
    except:
        pass
        
    return None

def extract_fssp_stats(lst):
    if not lst or not isinstance(lst, list):
        return 0, 0.0, 0, 0
    
    open_count = 0
    total_amount = 0.0
    has_tax = 0
    has_credit = 0
    
    for item in lst:
        if not isinstance(item, dict): continue
        
        status = str(item.get('status', '')).lower()
        amount = float(item.get('amount', 0) or 0)
        type_str = str(item.get('type', '')).lower()
        
        if 'открыто' in status or 'в производстве' in status or 'неоконченное' in status:
            open_count += 1
            total_amount += amount
            
            if any(kw in type_str for kw in ['налог', 'сбор', 'пфр', 'фсс', 'пеня']):
                has_tax = 1
            if any(kw in type_str for kw in ['кредит', 'ипотек', 'банковск']):
                has_credit = 1
                
    return open_count, total_amount, has_tax, has_credit

def check_bankruptcy_in_cases(lst):
    if not lst or not isinstance(lst, list):
        return 0
    txt = json.dumps(lst).lower()
    return int('банкрот' in txt or 'несостоятельност' in txt)

def main():
    print("📥 Загрузка данных...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f" Исходный размер: {df.shape}")
    
    # 1. Статусы и Даты
    print("\n Обработка статусов...")
    # Теперь status_type будет содержать исходную строку (например, "ликвидация юридического лица")
    df[['status_type', 'status_date']] = df['status'].apply(lambda x: pd.Series(parse_status_full(x)))
    
    df['registration_date_parsed'] = df['registration_date'].apply(parse_russian_date)
    
    # Применяем надежный маппинг в коды 0-3
    df['status_egr_encoded'] = df['status_egr'].apply(map_status_egr)
    
    # Время жизни
    print("⏱️ Расчет времени жизни...")
    df[['lifetime_days', 'lifetime_years']] = df.apply(
        lambda row: pd.Series(calc_lifetime(row['registration_date_parsed'], row['status_date'], row['status_egr_encoded'])), 
        axis=1, result_type='expand'
    )
    
    # Флаги активности (на основе кодов)
    df['is_active'] = (df['status_egr_encoded'] == 0).astype(int)
    df['is_liquidated'] = (df['status_egr_encoded'] == 2).astype(int)
    df['is_bankrupt'] = (df['status_egr_encoded'] == 3).astype(int)
    df['has_critical_status'] = (df['status_egr_encoded'].isin([2, 3])).astype(int)

    # 2. ФССП
    print("🔍 Обработка ФССП...")
    if 'fssp_proceedings' in df.columns:
        df['fssp_parsed'] = df['fssp_proceedings'].apply(safe_json_load)
        stats = df['fssp_parsed'].apply(extract_fssp_stats)
        df['fssp_open_count'] = stats.apply(lambda x: x[0])
        df['fssp_total_debt'] = stats.apply(lambda x: x[1])
        df['fssp_has_tax_debt'] = stats.apply(lambda x: x[2])
        df['fssp_has_credit_debt'] = stats.apply(lambda x: x[3])
        df.drop(columns=['fssp_parsed'], inplace=True)
    else:
        df['fssp_open_count'] = 0
        df['fssp_total_debt'] = 0.0
        df['fssp_has_tax_debt'] = 0
        df['fssp_has_credit_debt'] = 0

    df['has_open_fssp'] = (df['fssp_open_count'] > 0).astype(int)

    # 3. Суды и Банкротства
    print("⚖️ Обработка судов...")
    if 'all_cases_list' in df.columns:
        df['cases_parsed'] = df['all_cases_list'].apply(safe_json_load)
        df['has_bankruptcy_case'] = df['cases_parsed'].apply(check_bankruptcy_in_cases)
        df.drop(columns=['cases_parsed'], inplace=True)
    else:
        df['has_bankruptcy_case'] = 0

    # 4. Финансы
    print(" Обработка финансов...")
    for col in ['finance_revenue_by_year', 'finance_net_profit_by_year']:
        if col in df.columns:
            df[f'{col}_parsed'] = df[col].apply(safe_json_load)
            for year in ['2018', '2020', '2022', '2024']:
                df[f'{col}_{year}'] = df[f'{col}_parsed'].apply(lambda d: float(d.get(year, 0)) if isinstance(d, dict) else 0.0)
            
            if f'{col}_2018' in df.columns and f'{col}_2024' in df.columns:
                base = df[f'{col}_2018'].replace(0, 1)
                df[f'{col}_growth'] = (df[f'{col}_2024'] - df[f'{col}_2018']) / base
            df.drop(columns=[f'{col}_parsed'], inplace=True)

    # 5. Сотрудники
    emp_cols = [f'employees_{y}' for y in ['2018', '2020', '2022', '2024']]
    if all(c in df.columns for c in emp_cols):
        df['employees_growth'] = (df['employees_2024'].fillna(0) - df['employees_2018'].fillna(0)) / (df['employees_2018'].fillna(1).replace(0, 1))
        df['employees_avg'] = df[emp_cols].mean(axis=1)

    # 6. Интенсивность судов
    if 'courts_total_cases' in df.columns:
        df['court_intensity'] = df['courts_total_cases'] / (df['lifetime_years'].fillna(1).replace(0, 1))
        df['courts_total_cases_log'] = np.log1p(df['courts_total_cases'])
    
    # 7. ФИНАЛЬНЫЙ ОТБОР КОЛОНОК
    keep_cols = [
        'inn', 'ogrn', 'short_name', 'full_name', 'okved_main_code', 'okved_main_name',
        'registration_date', 'registration_date_parsed', 'lifetime_days', 'lifetime_years', 
        'status_type', 'status_date', 'is_active', 'status_egr_encoded', 'has_critical_status',
        'address', 'address_valid', 'director_name', 'director_inn', 'director_position',
        'employees_2018', 'employees_2020', 'employees_2022', 'employees_2024', 'employees_growth', 'employees_avg',
        'courts_total_cases', 'courts_total_cases_log', 'first_case_number', 'first_case_amount', 
        'first_case_category', 'first_case_status', 'first_case_kad_url', 'first_case_pdf_text',
        'court_intensity', 'has_bankruptcy_case',
        'fssp_total_count', 'fssp_total_count_log', 'fssp_total_amount', 'fssp_total_debt',
        'fssp_open_count', 'has_open_fssp', 'fssp_has_tax_debt', 'fssp_has_credit_debt', 'fssp_proceedings',
        'finance_revenue_by_year', 'finance_net_profit_by_year',
        'finance_revenue_by_year_growth', 'finance_net_profit_by_year_growth',
    ]
    
    if 'tax_debts_count' in df.columns:
        keep_cols.extend(['tax_debts_count', 'tax_debts_count_log'])
        df['tax_debts_count_log'] = np.log1p(df['tax_debts_count'])

    final_df = df[[c for c in keep_cols if c in df.columns]].copy()
    
    numeric_cols = final_df.select_dtypes(include=[np.number]).columns
    final_df[numeric_cols] = final_df[numeric_cols].fillna(0).replace([np.inf, -np.inf], 0)
    
    text_cols = ['first_case_pdf_text', 'fssp_proceedings', 'finance_revenue_by_year', 'finance_net_profit_by_year']
    for col in text_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna('')

    final_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"\n✅ Сохранено: {OUTPUT_CSV}")
    print(f"   Строк: {len(final_df)}, Столбцов: {len(final_df.columns)}")
    
    print(f"\n Проверка распределения статусов:")
    print(final_df['status_egr_encoded'].value_counts())

if __name__ == "__main__":
    main()