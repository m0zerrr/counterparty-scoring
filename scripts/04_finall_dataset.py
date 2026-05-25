import pandas as pd
import numpy as np
import re
import json
import warnings
from datetime import datetime
warnings.simplefilter('ignore')

INPUT_CSV = 'data/intermediate/cleaned_data_with_text.csv'
OUTPUT_CSV = 'data/intermediate/companies_ready.csv'

# Месяцы для парсинга дат
MONTHS = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

def parse_russian_date(date_str):
    """Парсит дату в формате '2 октября 2002' или '10/2/02'"""
    if pd.isna(date_str) or str(date_str).strip() == '':
        return pd.NaT
    
    date_str = str(date_str).strip().lower()
    
    # Пробуем формат "2 октября 2002"
    for month_name, month_num in MONTHS.items():
        if month_name in date_str:
            # Заменяем месяц на номер
            pattern = r'(\d{1,2})\s+' + month_name + r'\s+(\d{4})'
            match = re.search(pattern, date_str)
            if match:
                day, year = match.groups()
                try:
                    return pd.Timestamp(year=int(year), month=month_num, day=int(day))
                except:
                    return pd.NaT
    
    # Пробуем формат "10/2/02" или "2/10/2002"
    try:
        # Пробуем разные форматы
        for fmt in ['%m/%d/%y', '%d/%m/%Y', '%m/%d/%Y']:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
    except:
        pass
    
    return pd.NaT

def parse_status_full(status_str):
    """Парсит статус и извлекает дату если есть"""
    if pd.isna(status_str) or str(status_str).strip() == '':
        return 'unknown', pd.NaT
    
    s = str(status_str).strip()
    
    if ',' in s:
        parts = s.split(',', 1)
        status_type = parts[0].strip().lower()
        date_str = parts[1].strip()
        
        # Парсим дату из статуса
        status_date = parse_russian_date(date_str)
        return status_type, status_date
    else:
        return s.lower(), pd.NaT

def calc_lifetime(reg_date, status_date, status_type):
    """Рассчитывает время жизни компании"""
    if pd.isna(reg_date):
        return np.nan, np.nan
    
    # Определяем конечную дату
    if pd.notna(status_date):
        end_date = status_date
    elif status_type in ['ликвидирована', 'ликвидировано', 'прекратило деятельность']:
        # Если статус ликвидация но даты нет - берем сегодня
        end_date = pd.Timestamp.today()
    else:
        # Действующая компания
        end_date = pd.Timestamp.today()
    
    lifetime_days = (end_date - reg_date).days
    lifetime_years = round(lifetime_days / 365.25, 2)
    
    return lifetime_days, lifetime_years

def safe_json_load(x):
    if pd.isna(x) or x == "": 
        return None
    try:
        return json.loads(str(x).replace("'", '"'))
    except:
        return None

def main():
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f'Исходный размер: {df.shape}')
    print(f'Пример registration_date: {df["registration_date"].iloc[0] if "registration_date" in df.columns else "НЕТ КОЛОНКИ"}')
    
    # 1. Парсинг даты регистрации (ИСПРАВЛЕНО)
    print('\n📅 Парсинг даты регистрации...')
    df['registration_date_parsed'] = df['registration_date'].apply(parse_russian_date)
    
    # Проверяем сколько дат распарсилось
    parsed_count = df['registration_date_parsed'].notna().sum()
    print(f'✅ Распарсено дат: {parsed_count}/{len(df)} ({parsed_count/len(df)*100:.1f}%)')
    
    # 2. Парсинг статуса
    print('\n📋 Парсинг статуса...')
    df[['status_type', 'status_date']] = df['status'].apply(
        lambda x: pd.Series(parse_status_full(x))
    )
    
    # 3. Расчет времени жизни компании (НОВОЕ)
    print('\n⏱️ Расчет времени жизни компании...')
    df[['lifetime_days', 'lifetime_years']] = df.apply(
        lambda row: pd.Series(calc_lifetime(
            row['registration_date_parsed'], 
            row['status_date'], 
            row['status_type']
        )), 
        axis=1, 
        result_type='expand'
    )
    
    # 4. Активность компании
    active_keywords = ['действует', 'действующее', 'активна', 'работает']
    df['is_active'] = df['status_type'].isin(active_keywords).astype(int)
    
    # 5. Кодировка status_egr
    egr_map = {
        "Действующее": 0, "В процессе ликвидации": 1, "В процессе банкротства": 2,
        "Реорганизация": 2, "Ликвидировано": 3, "Банкротство": 3,
        "Исключение из ЕГРЮЛ недействующего юридического лица": 3,
        "Регистрация признана недействительной": 3, "Недействующее": 3
    }
    df['status_egr_encoded'] = df['status_egr'].map(egr_map).fillna(-1).astype(int)
    df['has_critical_status'] = (df['status_egr_encoded'] >= 2).astype(int)
    
    # 6. Агрегации
    if 'tax_debts_count' in df.columns:
        df['has_tax_debt'] = (df['tax_debts_count'] > 0).astype(int)
        df['tax_debts_sum'] = 0.0
        df['tax_debts_count_log'] = np.log1p(df['tax_debts_count'])

    if 'fssp_proceedings' in df.columns:
        def count_open_fssp(x):
            lst = safe_json_load(x)
            if not lst: 
                return 0
            return sum(1 for p in lst if isinstance(p, dict) and str(p.get('status', '')).lower() in ['открыто', 'в производстве'])
        df['fssp_open_count'] = df['fssp_proceedings'].apply(count_open_fssp)
        df['has_open_fssp'] = (df['fssp_open_count'] > 0).astype(int)

    if 'all_cases_list' in df.columns:
        def has_bankruptcy(x):
            lst = safe_json_load(x)
            if not lst: 
                return 0
            txt = json.dumps(lst).lower()
            return int('банкрот' in txt or 'несостоятельност' in txt)
        df['has_bankruptcy_case'] = df['all_cases_list'].apply(has_bankruptcy)

    # 7. Финансы
    for col in ['finance_revenue_by_year', 'finance_net_profit_by_year']:
        if col in df.columns:
            df[f'{col}_parsed'] = df[col].apply(safe_json_load)
            for year in ['2018', '2020', '2022', '2024']:
                df[f'{col}_{year}'] = df[f'{col}_parsed'].apply(lambda d: float(d.get(year, 0)) if d else 0.0)
            
            if col == 'finance_revenue_by_year' and 'finance_revenue_by_year_2018' in df.columns:
                df['revenue_growth_24_vs_18'] = (df['finance_revenue_by_year_2024'] - df['finance_revenue_by_year_2018']) / (df['finance_revenue_by_year_2018'].replace(0, 1))
            if col == 'finance_net_profit_by_year' and 'finance_net_profit_by_year_2018' in df.columns:
                df['profit_volatility'] = df[[f'finance_net_profit_by_year_{y}' for y in ['2018','2020','2022','2024']]].std(axis=1)
            df.drop(columns=[f'{col}_parsed'], inplace=True)

    # 8. Сотрудники
    if all(f'employees_{y}' in df.columns for y in ['2018', '2024']):
        df['employees_growth'] = (df['employees_2024'].fillna(0) - df['employees_2018'].fillna(0)) / (df['employees_2018'].fillna(1).replace(0, 1))
        df['employees_avg'] = df[[f'employees_{y}' for y in ['2018','2020','2022','2024']]].mean(axis=1)

    # 9. Интенсивность судов
    if 'lifetime_years' in df.columns and 'courts_total_cases' in df.columns:
        df['court_intensity'] = df['courts_total_cases'] / (df['lifetime_years'].fillna(1).replace(0, 1))
    
    # 10. Лог-преобразования
    for col in ['tax_debts_count', 'courts_total_cases', 'fssp_total_count']:
        if col in df.columns:
            df[f'{col}_log'] = np.log1p(df[col])

    # 11. Булевы в int
    bool_cols = df.select_dtypes(include=['bool']).columns
    df[bool_cols] = df[bool_cols].astype(int)

    # 12. ФИНАЛЬНЫЙ ОТБОР КОЛОНОК (УДАЛЕНЫ first_case_defendants, first_case_plaintiffs, all_cases_list)
    keep_cols = [
        # Идентификаторы
        'inn', 'ogrn', 'short_name', 'full_name',
        'okved_main_code', 'okved_main_name',
        
        # Даты и статусы
        'registration_date', 'registration_date_parsed',
        'lifetime_days', 'lifetime_years', 
        'status_type', 'status_date',
        'is_active',
        'status_egr_encoded', 'has_critical_status',
        
        # Адрес и директор
        'address', 'address_valid',
        'director_name', 'director_inn', 'director_position',
        
        # Сотрудники
        'employees_2018', 'employees_2019', 'employees_2020', 
        'employees_2021', 'employees_2022', 'employees_2023', 'employees_2024',
        'employees_growth', 'employees_avg',
        
        # СУДЫ
        'courts_total_cases', 'courts_total_cases_log',
        'first_case_number', 'first_case_amount', 
        'first_case_category',
        'first_case_status', 'first_case_kad_url',
        'first_case_pdf_text',  # ТЕКСТ - ОЧЕНЬ ВАЖНО!
        'court_intensity', 'has_bankruptcy_case',
        
        # ФССП
        'fssp_total_count', 'fssp_total_count_log',
        'fssp_total_amount', 'fssp_open_count', 'has_open_fssp',
        'fssp_proceedings',
        
        # Налоги
        'taxation_has_data', 'taxes_count', 'tax_debts_count',
        'tax_debts_count_log', 'has_tax_debt', 'tax_debts_sum',
        
        # Финансы
        'finance_has_data', 'finance_revenue', 'finance_profit',
        'finance_revenue_by_year', 'finance_net_profit_by_year',
        'revenue_growth_24_vs_18', 'profit_volatility',
        
        # Флаги
        'has_all_cases_list_data', 'has_taxes_list_data',
        'has_fssp_proceedings_data', 'has_finance_revenue_by_year_data',
        'has_finance_net_profit_by_year_data'
    ]
    
    final_df = df[[c for c in keep_cols if c in df.columns]].copy()
    
    # Заполнение пропусков
    numeric_cols = final_df.select_dtypes(include=[np.number]).columns
    final_df[numeric_cols] = final_df[numeric_cols].fillna(0)
    final_df[numeric_cols] = final_df[numeric_cols].replace([np.inf, -np.inf], 0)
    
    # Текстовые поля
    text_cols = ['first_case_pdf_text', 'fssp_proceedings', 
                 'finance_revenue_by_year', 'finance_net_profit_by_year']
    for col in text_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna('')

    final_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"\n✅ Сохранено: {OUTPUT_CSV}")
    print(f"   Строк: {len(final_df)}, Столбцов: {len(final_df.columns)}")
    
    # Проверка
    print(f"\n📊 Проверка данных:")
    print(f"   registration_date_parsed пропусков: {final_df['registration_date_parsed'].isna().sum() if 'registration_date_parsed' in final_df.columns else 'НЕТ КОЛОНКИ'}")
    print(f"   lifetime_years (среднее): {final_df['lifetime_years'].mean() if 'lifetime_years' in final_df.columns else 'НЕТ КОЛОНКИ'}")
    print(f"   is_active: {final_df['is_active'].value_counts().to_dict() if 'is_active' in final_df.columns else 'НЕТ КОЛОНКИ'}")
    
    if 'first_case_defendants' in final_df.columns:
        print("   ⚠️ first_case_defendants НЕ УДАЛЕН!")
    if 'first_case_plaintiffs' in final_df.columns:
        print("   ⚠️ first_case_plaintiffs НЕ УДАЛЕН!")
    if 'all_cases_list' in final_df.columns:
        print("   ⚠️ all_cases_list НЕ УДАЛЕН!")

if __name__ == "__main__":
    main()