import requests
import pandas as pd
import json
import re
import time
from bs4 import BeautifulSoup

class Parser():
    def __init__(self, delay=1):
        self.base_url = 'https://datanewton.ru'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        })
        self.delay = delay
        self.inn_data = pd.read_csv('companies.csv', dtype=str)
    
    def get_company_data(self, inn):
        ogrn = self._get_ogrn(inn)
        base_company_url = f'{self.base_url}/contragents/{ogrn}'
        print(base_company_url)
        
        main_soup = self._parse_page(base_company_url)
        json_data = self._extract_json_from_html(main_soup)
        
        if json_data:
            return self._parse_contragent_from_json(json_data, inn, ogrn)
        else:
            company_data = {
                'inn': inn,
                'ogrn': ogrn,
                'general': main_soup,
                'courts': self._parse_page(f'{base_company_url}/courts'),
                'taxation': self._parse_page(f'{base_company_url}/taxation'),
                'fssp': self._parse_page(f'{base_company_url}/fssp'),
                'finance': self._parse_page(f'{base_company_url}/finance')
            }
            
            return {
                'inn': inn,
                'ogrn': ogrn,
                'general': self._extract_general(company_data['general']),
                'courts': self._extract_courts(company_data['courts']),
                'taxation': self._extract_taxation(company_data['taxation']),
                'fssp': self._extract_fssp(company_data['fssp']),
                'finance': self._extract_finance(company_data['finance'])
            }
    
    def _parse_page(self, url):
        time.sleep(self.delay)
        response = self.session.get(url, timeout=30)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')
        return None
    
    def _extract_json_from_html(self, soup):
        if not soup:
            return None
        
        script_tag = soup.find('script', id='_R_')
        if not script_tag or not script_tag.string:
            return None
        
        json_pattern = r'"loaderData",(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})'
        json_match = re.search(json_pattern, script_tag.string, re.DOTALL)
        
        if not json_match:
            return None
        
        return json.loads(json_match.group(1))
    
    def _parse_contragent_from_json(self, json_data, inn, ogrn):
        contragent = json_data["loaderData"]["routes/contragents.$ogrn.($tab).($sub)"]["contragent"]
        
        general = contragent.get("general", {})
        managers = contragent.get("managers", [])
        owners = contragent.get("owners", [])
        aggregations = contragent.get("aggregations", {})
        
        directors = []
        for m in managers:
            directors.append({
                "name": m.get("fio"),
                "inn": m.get("inn"),
                "position": m.get("position"),
                "start_date": m.get("start_date")
            })
        
        shareholders = []
        for o in owners:
            shareholders.append({
                "name": o.get("name"),
                "share": o.get("share"),
                "inn": o.get("inn")
            })
        
        okveds = []
        for okved in general.get("okveds", []):
            if okved.get("main"):
                okveds.append({
                    "code": okved.get("code"),
                    "name": okved.get("value")
                })
        
        return {
            'inn': inn,
            'ogrn': ogrn,
            'short_name': general.get("short_name"),
            'full_name': general.get("full_name"),
            'status': general.get("status", {}).get("status_rus_short"),
            'status_egr': general.get("status", {}).get("status_egr"),
            'registration_date': general.get("ogrn_date"),
            'liquidation_date': general.get("liquidation_date"),
            'address': self._clean_text(general.get("address")),
            'address_valid': len(general.get("address_false_info_details", [])) == 0,
            'directors': directors,
            'shareholders': shareholders,
            'okveds': okveds,
            'employees_2018': general.get("workers_count", {}).get("2018"),
            'employees_2019': general.get("workers_count", {}).get("2019"),
            'employees_2020': general.get("workers_count", {}).get("2020"),
            'has_arbitration': aggregations.get("has_arbitration_cases", False),
            'arbitration_cases_count': aggregations.get("arbitration_cases_count", 0),
            'has_enforcement': aggregations.get("has_enforcement_proceedings", False),
            'enforcement_count': aggregations.get("enforcement_proceedings_count", 0),
            'has_mass_director': self._detect_mass_directors(managers)
        }
    
    def _detect_mass_directors(self, managers, threshold=5):
        mass_count = sum(1 for m in managers if m.get("mass_owner", False))
        return mass_count > 0
    
    def _clean_text(self, text):
        """Очистка текста от мусора типа 'content_copy'"""
        if not text:
            return None
        # Удаляем "content_copy" и лишние пробелы
        cleaned = re.sub(r'content_copy\s*', '', text)
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned.strip()
    
    def _extract_general(self, soup):
        if not soup:
            return None
        
        data = {}
        
        # Краткое наименование из h1
        h1 = soup.find("h1")
        if h1:
            data['short_name'] = self._clean_text(h1.get_text().split("ИНН")[0].strip())
        
        # Извлечение из таблиц
        for td in soup.find_all("td"):
            text = td.get_text().strip()
            
            if "ИНН" in text:
                next_td = td.find_next_sibling("td")
                if next_td:
                    data['inn'] = self._clean_text(next_td.get_text())
            
            elif "ОГРН" in text:
                next_td = td.find_next_sibling("td")
                if next_td:
                    ogrn_text = self._clean_text(next_td.get_text())
                    # Оставляем только цифры ОГРН
                    ogrn_match = re.search(r'\d{13,15}', ogrn_text)
                    data['ogrn'] = ogrn_match.group(0) if ogrn_match else ogrn_text
            
            elif "Статус" in text:
                next_td = td.find_next_sibling("td")
                if next_td:
                    data['status'] = self._clean_text(next_td.get_text())
            
            elif "Полное наименование" in text:
                next_td = td.find_next_sibling("td")
                if next_td:
                    data['full_name'] = self._clean_text(next_td.get_text())
            
            elif "Юридический адрес" in text:
                next_td = td.find_next_sibling("td")
                if next_td:
                    address_text = self._clean_text(next_td.get_text())
                    # Убираем цифры в конце (количество компаний по адресу)
                    address_text = re.sub(r'\d+$', '', address_text).strip()
                    data['address'] = address_text
        
        return data
    
    def _extract_courts(self, soup):
        if not soup:
            return {'cases': [], 'total_cases': 0}
        
        cases = []
        
        # Поиск блоков с делами (разные классы на разных страницах)
        case_blocks = soup.find_all("div", class_=re.compile(r"case|card|item"))
        
        for block in case_blocks:
            case = {}
            
            # Номер дела
            number_elem = block.find("div", string=re.compile(r"А\d{2}-\d+/\d{4}"))
            if number_elem:
                case['case_number'] = self._clean_text(number_elem)
            else:
                number_elem = block.find("a", href=re.compile(r"/card\?number="))
                if number_elem:
                    case['case_number'] = self._clean_text(number_elem.get_text())
            
            if case:
                cases.append(case)
        
        return {
            'cases': cases,
            'total_cases': len(cases)
        }
    
    def _extract_taxation(self, soup):
        if not soup:
            return {'has_data': False}
        
        # Проверяем, есть ли таблица с налогами
        tax_table = soup.find("div", string=re.compile(r"Уплаченные налоги"))
        return {'has_data': tax_table is not None}
    
    def _extract_fssp(self, soup):
        if not soup:
            return {'proceedings': [], 'total_count': 0}
        
        proceedings = []
        
        # Поиск исполнительных производств
        proc_blocks = soup.find_all("div", class_=re.compile(r"proceeding|enforcement"))
        
        for block in proc_blocks:
            proceeding = {}
            
            # Номер производства
            number_elem = block.find("div", string=re.compile(r"№ \d+/\d+/\d+"))
            if number_elem:
                proceeding['number'] = self._clean_text(number_elem)
            
            # Сумма
            amount_elem = block.find("div", string=re.compile(r"[\d\s]+₽"))
            if amount_elem:
                amount_text = self._clean_text(amount_elem)
                numbers = re.findall(r"[\d]+", amount_text)
                if numbers:
                    proceeding['amount'] = float(numbers[0])
            
            if proceeding:
                proceedings.append(proceeding)
        
        return {
            'proceedings': proceedings,
            'total_count': len(proceedings)
        }
    
    def _extract_finance(self, soup):
        if not soup:
            return {'has_data': False}
        
        # Проверяем, есть ли финансовая информация
        finance_table = soup.find("div", string=re.compile(r"Финансовые результаты|Выручка"))
        return {'has_data': finance_table is not None}
    
    def _get_ogrn(self, inn):
        return self.inn_data[self.inn_data['inn'] == inn]['ogrn'].values[0]

    def collect_all_companies(self, input_file='companies.csv', output_file='companies_data.json', output_format='json'):
        """
        Сбор данных по всем компаниям из файла
        """
        # Чтение списка ИНН
        df = pd.read_csv(input_file, dtype=str)
        
        if 'inn' not in df.columns:
            raise ValueError(f"Файл {input_file} должен содержать колонку 'inn'")
        
        inn_list = df['inn'].dropna().unique().tolist()
        print(f"Найдено {len(inn_list)} уникальных ИНН")
        
        results = []
        success_count = 0
        error_count = 0
        
        for i, inn in enumerate(inn_list):
            print(f"\n[{i+1}/{len(inn_list)}] Обработка ИНН: {inn}")
            
            data = self.get_company_data(inn)
            
            if data:
                results.append(data)
                success_count += 1
                print(f"  ✓ Успешно")
            else:
                error_count += 1
                print(f"  ✗ Нет данных")
            
            time.sleep(self.delay)
            
            # Промежуточное сохранение каждые 10 компаний
            if (i + 1) % 10 == 0:
                self._save_results(results, output_file, output_format)
                print(f"  💾 Промежуточное сохранение ({i+1} компаний)")
        
        # Финальное сохранение
        self._save_results(results, output_file, output_format)
        
        print(f"\n{'='*50}")
        print(f"Сбор завершен!")
        print(f"Успешно: {success_count}")
        print(f"Ошибок: {error_count}")
        print(f"Результаты сохранены в: {output_file}")
        
        return results

    def _save_results(self, results, output_file, output_format):
        """
        Сохранение результатов в файл с полным раскрытием всех полей
        """
        if output_format == 'json':
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        elif output_format == 'csv':
            flat_results = []
            for item in results:
                flat_item = {}
                
                # Базовые поля
                flat_item['inn'] = item.get('inn')
                flat_item['ogrn'] = item.get('ogrn')
                flat_item['short_name'] = item.get('short_name')
                flat_item['full_name'] = item.get('full_name')
                flat_item['status'] = item.get('status')
                flat_item['status_egr'] = item.get('status_egr')
                flat_item['registration_date'] = item.get('registration_date')
                flat_item['liquidation_date'] = item.get('liquidation_date')
                flat_item['address'] = item.get('address')
                flat_item['address_valid'] = item.get('address_valid')
                flat_item['has_arbitration'] = item.get('has_arbitration')
                flat_item['arbitration_cases_count'] = item.get('arbitration_cases_count')
                flat_item['has_enforcement'] = item.get('has_enforcement')
                flat_item['enforcement_count'] = item.get('enforcement_count')
                flat_item['has_mass_director'] = item.get('has_mass_director')
                flat_item['employees_2018'] = item.get('employees_2018')
                flat_item['employees_2019'] = item.get('employees_2019')
                flat_item['employees_2020'] = item.get('employees_2020')
                
                # Раскрытие директоров (берем первого)
                directors = item.get('directors', [])
                if directors:
                    flat_item['director_name'] = directors[0].get('name')
                    flat_item['director_inn'] = directors[0].get('inn')
                    flat_item['director_position'] = directors[0].get('position')
                    flat_item['director_start_date'] = directors[0].get('start_date')
                else:
                    flat_item['director_name'] = None
                    flat_item['director_inn'] = None
                    flat_item['director_position'] = None
                    flat_item['director_start_date'] = None
                
                # Раскрытие участников (берем первого)
                shareholders = item.get('shareholders', [])
                if shareholders:
                    flat_item['shareholder_name'] = shareholders[0].get('name')
                    flat_item['shareholder_share'] = shareholders[0].get('share')
                    flat_item['shareholder_inn'] = shareholders[0].get('inn')
                else:
                    flat_item['shareholder_name'] = None
                    flat_item['shareholder_share'] = None
                    flat_item['shareholder_inn'] = None
                
                # Раскрытие ОКВЭД (берем первый)
                okveds = item.get('okveds', [])
                if okveds:
                    flat_item['okved_main_code'] = okveds[0].get('code')
                    flat_item['okved_main_name'] = okveds[0].get('name')
                else:
                    flat_item['okved_main_code'] = None
                    flat_item['okved_main_name'] = None
                
                # Количество директоров и участников
                flat_item['directors_count'] = len(directors)
                flat_item['shareholders_count'] = len(shareholders)
                flat_item['okveds_count'] = len(okveds)
                
                # Данные из вкладок (fallback)
                general = item.get('general', {})
                if isinstance(general, dict):
                    flat_item['general_status'] = general.get('status')
                    flat_item['general_inn'] = general.get('inn')
                    flat_item['general_ogrn'] = general.get('ogrn')
                    flat_item['general_address'] = general.get('address')
                    flat_item['general_short_name'] = general.get('short_name')
                    flat_item['general_full_name'] = general.get('full_name')
                
                courts = item.get('courts', {})
                if isinstance(courts, dict):
                    flat_item['courts_total_cases'] = courts.get('total_cases', 0)
                    flat_item['courts_cases_list'] = json.dumps(courts.get('cases', []), ensure_ascii=False)
                
                taxation = item.get('taxation', {})
                if isinstance(taxation, dict):
                    flat_item['taxation_has_data'] = taxation.get('has_data', False)
                
                fssp = item.get('fssp', {})
                if isinstance(fssp, dict):
                    flat_item['fssp_total_count'] = fssp.get('total_count', 0)
                    flat_item['fssp_total_amount'] = sum(p.get('amount', 0) for p in fssp.get('proceedings', []))
                    flat_item['fssp_proceedings_list'] = json.dumps(fssp.get('proceedings', []), ensure_ascii=False)
                
                finance = item.get('finance', {})
                if isinstance(finance, dict):
                    flat_item['finance_has_data'] = finance.get('has_data', False)
                
                # Ошибка если есть
                flat_item['error'] = item.get('error')
                
                flat_results.append(flat_item)
            
            df = pd.DataFrame(flat_results)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        else:
            raise ValueError(f"Неподдерживаемый формат: {output_format}")

# Использование
parser = Parser()
result = parser.collect_all_companies(
  input_file='companies.csv',
  output_file='companies_data.csv',
  output_format='csv'
)