import pandas as pd
import feedparser
import re
import time
from datetime import datetime, timedelta
import warnings

warnings.simplefilter('ignore')

INPUT_CSV = 'data/intermediate/companies_ready.csv'
OUTPUT_CSV = 'data/intermediate/companies_with_news.csv'

RSS_FEEDS = [
  'http://lenta.ru/rss/',
  'http://www.ria.ru/export/rss2/index.xml',
  'http://www.kommersant.ru/RSS/news.xml',
  'http://vz.ru/rss.xml',
  'http://tass.ru/rss/v2.xml?sections=MjU%3D',
  'http://www.rian.ru/export/rss2/index.xml',
]

negative_words = [
    'штраф', 'суд', 'иск', 'долг', 'банкрот',
    'ликвидация', 'мошенничество', 'арест', 'проверка', 
    'нарушение', 'уголовное дело', 'налоговая', 'фссп', 
    'взыскание', 'конфискация', 'растрата', 'хищение'
]

def clear_company_name(name):
  if not isinstance(name, str):
    return ''
  
  name = re.sub(r'ООО|ЗАО|ОАО|ПАО|НАО|АО\"|\'', '', name, flags=re.IGNORECASE)
  
  name = re.sub(r'[^\w\s]', ' ', name)
  words = [w for w in name.split() if len(w) > 3]
  return ' '.join(words[:3]) if words else name[:15]

def search_news(company_name, days_back=30):
  if not company_name or len(company_name) < 3:
    return 0, False
  
  count = 0
  has_negative = False
  cutoff_date = datetime.now() - timedelta(days=days_back)
  
  common_words = [
    'строй', "инвест", "торг", "сервис", "технологии", "группа", "системы", "ресурсы"
  ]
  if any(word in company_name.lower() for word in common_words) and len(company_name.split()) < 3:
    return 0, False
  
  for feed_url in RSS_FEEDS:
    try:
      feed = feedparser.parse(feed_url)
      for entry in feed.entries:
        if hasattr(entry, 'published_parsed'):
          pub_date = datetime(*entry.published_parsed[:6])
          if pub_date < cutoff_date:
            continue
          
        title = entry.get('title', '').lower()
        summary = entry.get('summary', '').lower()
        text = title + ' ' + summary
        
        if company_name.lower() in text:
          count += 1
          if any(kw in text for kw in negative_words):
            has_negative = True
            
    except Exception as e:
      continue
    
  return count, int(has_negative)

def main():
  df = pd.read_csv(INPUT_CSV, low_memory=False)
  
  df['search_name'] = df['short_name'].apply(clear_company_name)
  
  results = []
  total = len(df)
  
  start_time = time.time()
  
  for i, row in df.iterrows():
    name = row['search_name']
    mentions, negative = search_news(name, days_back=1825)
    results.append((mentions, negative))
    
    if (i + 1) % 50 == 0:
      elapsed = time.time() - start_time
      speed = elapsed / (i + 1)
      remainig = speed * (total - i - 1)
      print(f'Обработано {i+1}/{total} компаний. Осталось {remainig}//60 минут')
      
  df['news_mentions_count'] = [r[0] for r in results]
  df['has_negative_news'] = [r[1] for r in results]
  
  df.drop(columns=['search_name'], inplace=True)
  
  df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
  
if __name__ == '__main__':
  main()