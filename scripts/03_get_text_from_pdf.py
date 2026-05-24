import csv
import time
import os
import random
import json
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
import PyPDF2

# --- НАСТРОЙКИ ---
# Теперь читаем из промежуточного файла (где уже есть сохранённые тексты)
CSV_FILE = "data/intermediate/companies_data_with_text.csv"
SOURCE_BACKUP = "data/source/companies_data.csv"  # Резервный источник
OUTPUT_CSV = "data/intermediate/companies_data_with_text.csv"
DOWNLOAD_DIR = Path("data/intermediate/pdf_downloads")
CACHE_FILE = Path("data/intermediate/pdf_cache.json")
MAX_CASES = 20
MAX_RETRIES = 3

def human_pause(min_sec=10, max_sec=25):
    time.sleep(random.uniform(min_sec, max_sec))

def between_cases():
    time.sleep(random.uniform(120, 300))

def short_pause():
    time.sleep(random.uniform(2, 5))

def setup_driver():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    prefs = {
        "download.default_directory": str(DOWNLOAD_DIR.absolute()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def load_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def wait_for_download(before_files, timeout=45):
    for _ in range(timeout):
        time.sleep(1)
        after_files = set(DOWNLOAD_DIR.glob("*.pdf"))
        new_files = after_files - before_files
        if new_files:
            pdf_file = list(new_files)[0]
            if pdf_file.stat().st_size > 0:
                return pdf_file
    return None

def extract_case_text(driver, case_url, case_id, cache):
    if case_id in cache:
        entry = cache[case_id]
        if entry.get('retries', 0) >= MAX_RETRIES:
            print(f"  Пропускаем (лимит попыток: {entry['retries']})")
            return None
        else:
            prev_error = entry.get('error', '')
            if prev_error:
                print(f"  Повторная попытка #{entry['retries'] + 1} (предыдущая ошибка: {prev_error})")
            else:
                print(f"  Повторная попытка #{entry['retries'] + 1}")
            entry['retries'] += 1
            entry['last_attempt'] = time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        cache[case_id] = {
            'retries': 1,
            'url': case_url,
            'last_attempt': time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    driver.get(case_url)
    human_pause()
    
    pdf_links = driver.find_elements(By.CSS_SELECTOR, "a[href$='.pdf'], a[href*='.pdf']")
    for link in pdf_links:
        pdf_url = link.get_attribute("href")
        if pdf_url and 'PdfDocument' in pdf_url:
            print(f"  Скачиваю PDF...")
            
            before_files = set(DOWNLOAD_DIR.glob("*.pdf"))
            
            driver.execute_script(f"window.open('{pdf_url}','_blank');")
            short_pause()
            
            driver.switch_to.window(driver.window_handles[-1])
            short_pause()
            
            driver.close()
            short_pause()
            
            driver.switch_to.window(driver.window_handles[0])
            
            pdf_file = wait_for_download(before_files)
            
            if pdf_file:
                print(f"  Скачано: {pdf_file.name} ({pdf_file.stat().st_size} байт)")
                
                try:
                    with open(pdf_file, 'rb') as f:
                        reader = PyPDF2.PdfReader(f)
                        text_parts = []
                        for page in reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)
                        
                        text = '\n\n'.join(text_parts) if text_parts else None
                        
                        if text:
                            print(f"  Извлечено {len(text)} символов")
                            del cache[case_id]
                            return text
                        else:
                            print(f"  Текст не извлечён")
                            cache[case_id]['error'] = 'no_text'
                            return None
                except Exception as e:
                    print(f"  Ошибка: {e}")
                    cache[case_id]['error'] = str(e)[:50]
                    return None
                finally:
                    if pdf_file.exists():
                        pdf_file.unlink()
            else:
                print(f"  Не дождались скачивания")
                cache[case_id]['error'] = 'download_timeout'
                return None
    
    selectors = ["div.case-content", "div.view_case", "div.content", "div.card-body"]
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                text = elem.text
                if text and len(text) > 200:
                    print(f"  HTML текст: {len(text)} символов")
                    del cache[case_id]
                    return text
        except:
            continue
    
    cache[case_id]['error'] = 'no_content'
    return None

def main():
    print("=" * 60)
    print("Парсер kad.arbitr.ru")
    print(f"Запуск: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Обработаем: {MAX_CASES} дел")
    print("Пауза между делами: 2-5 минут")
    print("=" * 60)
    
    # Пытаемся загрузить уже обработанный файл
    if os.path.exists(OUTPUT_CSV):
        csv_to_read = OUTPUT_CSV
        print(f"Читаем сохранённый прогресс: {csv_to_read}")
    elif os.path.exists(CSV_FILE):
        csv_to_read = CSV_FILE
        print(f"Читаем исходный файл: {csv_to_read}")
    elif os.path.exists(SOURCE_BACKUP):
        csv_to_read = SOURCE_BACKUP
        print(f"Читаем резервный файл: {csv_to_read}")
    else:
        print(f"Ошибка: ни один файл не найден")
        return
    
    cache = load_cache()
    print(f"В кэше проблемных: {len(cache)}")
    
    with open(csv_to_read, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
    
    # Добавляем колонку для текста, если её нет
    if 'first_case_pdf_text' not in fieldnames:
        fieldnames = list(fieldnames) + ['first_case_pdf_text']
        for row in rows:
            row['first_case_pdf_text'] = row.get('first_case_pdf_text', '')
    
    print(f"Всего записей: {len(rows)}")
    
    to_process = []
    for idx, row in enumerate(rows):
        url = row.get('first_case_kad_url', '').strip()
        # Проверяем, есть ли уже текст
        has_text = row.get('first_case_pdf_text', '')
        if url and url.startswith('http') and not has_text:
            case_id = url.split('/')[-1]
            if case_id in cache and cache[case_id].get('retries', 0) >= MAX_RETRIES:
                print(f"  Пропускаем (кэш): {case_id[:20]}...")
                continue
            to_process.append((idx, row, url, case_id))
    
    print(f"Дел на обработку: {len(to_process)}")
    
    to_process = to_process[:MAX_CASES]
    
    if not to_process:
        print("Нет дел для обработки")
        return
    
    driver = setup_driver()
    
    try:
        success_count = 0
        fail_count = 0
        
        for i, (idx, row, url, case_id) in enumerate(to_process):
            print(f"\n{'='*60}")
            print(f"[{i+1}/{len(to_process)}] {url[:80]}...")
            print(f"Время: {time.strftime('%H:%M:%S')}")
            print(f"{'='*60}")
            
            text = extract_case_text(driver, url, case_id, cache)
            
            if text:
                row['first_case_pdf_text'] = text
                success_count += 1
                print(f"  [OK] Успешно: {success_count}, ошибок: {fail_count}")
            else:
                fail_count += 1
                print(f"  [FAIL] Успешно: {success_count}, ошибок: {fail_count}")
                if case_id in cache:
                    print(f"    Попыток: {cache[case_id]['retries']}/{MAX_RETRIES}")
                    print(f"    Ошибка: {cache[case_id].get('error', 'unknown')}")
            
            # Сохраняем прогресс после каждого дела
            with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
            save_cache(cache)
            
            if i < len(to_process) - 1:
                pause = random.uniform(120, 300)
                print(f"\n  Пауза {int(pause//60)} мин {int(pause%60)} сек...")
                time.sleep(pause)
    
    except KeyboardInterrupt:
        print("\n[СТОП] Прогресс сохранён")
        save_cache(cache)
    
    finally:
        print("\nЗакрываю браузер...")
        driver.quit()
        save_cache(cache)
    
    success = sum(1 for row in rows if row.get('first_case_pdf_text'))
    print(f"\n{'='*60}")
    print(f"ГОТОВО!")
    print(f"Успешно за сессию: {success_count}")
    print(f"Всего успешно: {success}")
    print(f"В кэше: {len(cache)}")
    print(f"Результат: {OUTPUT_CSV}")
    print(f"Кэш: {CACHE_FILE}")
    print(f"Завершено: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()