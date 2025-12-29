# Гайд по тестированию Stage 3: Date-Based Pagination

## Обзор изменений

Реализована date-based пагинация для обхода бага API с offset. Теперь система:
- Разбивает диапазон дат на дневные интервалы
- Загружает трейды для каждого дня отдельно
- Фильтрует трейды по timestamp на клиенте
- Останавливается до того, как сработает баг offset (~1000-1500)

## Предварительные требования

1. Подключение к серверу через SSH
2. Активированное виртуальное окружение (`venv`)
3. Установленные зависимости (`pip install -e .`)
4. База данных инициализирована (Stage 2 пройден)

## Тестирование новой функциональности

### Шаг 1: Получить Market ID (если еще не получен)

```bash
# На сервере, в директории проекта
cd ~/projects/Polymarket_bot_2
source venv/bin/activate

# Получить список маркетов для события
python scripts/stage1_extract_markets.py "https://polymarket.com/event/epstein-client-list-released-in-2025-372"
```

Запишите `conditionId` (например: `0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01`)

### Шаг 2: Проверить текущее состояние БД

```bash
# Посмотреть сколько трейдов уже в БД
python scripts/view_trades.py "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01" 10
```

Запишите количество трейдов и самую старую дату.

### Шаг 3: Запустить загрузку с date-based пагинацией

```bash
# Запустить полную загрузку всех исторических трейдов
python scripts/fetch_all_trades.py "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
```

### Шаг 4: Наблюдать за процессом

Вы должны увидеть:

1. **Загрузка существующих трейдов из БД:**
   ```
   ✓ Loaded X existing unique trades for filtering
   ✓ Oldest trade in DB: YYYY-MM-DD HH:MM:SS
   ```

2. **Определение даты начала:**
   - Если API предоставляет дату создания:
     ```
     ✓ Found market creation date from API: YYYY-MM-DD HH:MM:SS
     ```
   - Если используется fallback:
     ```
     ⚠️  Using fallback start date: YYYY-MM-DD HH:MM:SS (oldest in DB - 1 day)
     ```
     или
     ```
     ⚠️  Using fixed fallback start date: YYYY-MM-DD HH:MM:SS
     ```

3. **Генерация дневных интервалов:**
   ```
   ✓ Generated N daily intervals from YYYY-MM-DD to YYYY-MM-DD
   ```

4. **Обработка каждого дня:**
   ```
   [1/N] Fetching trades for YYYY-MM-DD...
     ✓ Found X new trades for YYYY-MM-DD
   [2/N] Fetching trades for YYYY-MM-DD...
     - No new trades for YYYY-MM-DD
   ...
   ```

5. **Финальная статистика:**
   ```
   ✓ Finished processing N daily intervals.
     ✓ New trades loaded: X
     ✓ Duplicates skipped: Y
     ✓ Efficiency: Z.Z% unique
     ✓ Total volume in database: X,XXX.XX
   ```

### Шаг 5: Проверить результаты

```bash
# Посмотреть обновленное количество трейдов
python scripts/view_trades.py "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01" 20

# Проверить самую старую дату - должна быть старше чем была до загрузки
```

### Шаг 6: Проверить отсутствие дубликатов

```bash
# Посмотреть статистику по трейдам
python -c "
from src.database.connection import get_connection
conn = get_connection()
cursor = conn.cursor()

# Общее количество трейдов
cursor.execute('SELECT COUNT(*) FROM trades WHERE market_id = ?', ('0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01',))
total = cursor.fetchone()[0]
print(f'Total trades: {total:,}')

# Количество уникальных по signature
cursor.execute('''
    SELECT COUNT(DISTINCT timestamp || '|' || price || '|' || size || '|' || trader_address)
    FROM trades 
    WHERE market_id = ?
''', ('0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01',))
unique = cursor.fetchone()[0]
print(f'Unique trades: {unique:,}')
print(f'Duplicates: {total - unique:,}')

# Диапазон дат
cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM trades WHERE market_id = ?', ('0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01',))
min_ts, max_ts = cursor.fetchone()
from datetime import datetime
print(f'Date range: {datetime.fromtimestamp(min_ts).strftime(\"%Y-%m-%d\")} to {datetime.fromtimestamp(max_ts).strftime(\"%Y-%m-%d\")}')

conn.close()
"
```

## Что проверить

### ✅ Успешные сценарии:

1. **Загрузка начинается с правильной даты:**
   - Если в БД есть трейды, дата начала должна быть `min_timestamp_in_db - 1 день`
   - Если БД пустая, должна использоваться фиксированная дата (2024-11-19) или дата из API

2. **Генерация интервалов:**
   - Количество интервалов должно соответствовать количеству дней от даты начала до текущей даты
   - Каждый интервал должен быть один день

3. **Загрузка по дням:**
   - Для каждого дня выводится прогресс `[X/N]`
   - Показывается количество найденных новых трейдов или сообщение "No new trades"

4. **Отсутствие дубликатов:**
   - `Efficiency` должна быть близка к 100%
   - Количество `Duplicates skipped` должно быть минимальным

5. **Полнота данных:**
   - Самая старая дата в БД должна быть близка к дате начала маркета (19 ноября 2024)
   - Объем данных должен увеличиться по сравнению с предыдущей загрузкой

### ⚠️ Потенциальные проблемы:

1. **Скрипт останавливается слишком рано:**
   - Проверьте, что `max_offset=1000` не слишком мал
   - Проверьте логи на наличие предупреждений о достижении max_offset

2. **Много дубликатов:**
   - Проверьте, что `seen_signatures` правильно загружается из БД
   - Убедитесь, что `create_trade_signature` работает корректно

3. **Не загружаются старые трейды:**
   - Проверьте fallback дату начала
   - Убедитесь, что `generate_daily_intervals` генерирует правильные интервалы

4. **Ошибки API:**
   - Проверьте, что `get_market_info` правильно обрабатывает ошибки
   - Убедитесь, что rate limiting работает корректно

## Сравнение с предыдущей версией

### Старая версия (offset-based):
- Останавливалась на ~906-1000 трейдах
- Возвращала дубликаты после offset ~1000-1500
- Не могла загрузить всю историю

### Новая версия (date-based):
- Загружает трейды по дням
- Фильтрует по timestamp на клиенте
- Останавливается до бага offset
- Должна загрузить всю доступную историю

## Дополнительные тесты

### Тест 1: Загрузка для конкретного дня

Можно модифицировать скрипт для тестирования одного дня:

```python
# Временный тест-скрипт
import asyncio
from datetime import datetime, timezone
from src.parser.trade_parser import async_fetch_trades_by_date_range
from src.parser.api_client import AsyncPolymarketAPIClient

async def test_single_day():
    condition_id = "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
    # Тестируем один день: 2024-12-29
    start = int(datetime(2024, 12, 29, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2024, 12, 30, tzinfo=timezone.utc).timestamp())
    
    async with AsyncPolymarketAPIClient() as client:
        trades = await async_fetch_trades_by_date_range(condition_id, start, end, client)
        print(f"Found {len(trades)} trades for 2024-12-29")

asyncio.run(test_single_day())
```

### Тест 2: Проверка генерации интервалов

```python
from src.parser.trade_parser import generate_daily_intervals
from datetime import datetime, timezone

start = int(datetime(2024, 11, 19, tzinfo=timezone.utc).timestamp())
end = int(datetime(2024, 12, 30, tzinfo=timezone.utc).timestamp())

intervals = generate_daily_intervals(start, end)
print(f"Generated {len(intervals)} intervals")
for i, (s, e) in enumerate(intervals[:5]):  # Первые 5
    print(f"  {i+1}. {datetime.fromtimestamp(s).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(e).strftime('%Y-%m-%d')}")
```

## Ожидаемые результаты

После успешного тестирования вы должны увидеть:

1. ✅ Загрузка всех доступных исторических трейдов (не останавливается на ~900)
2. ✅ Минимальное количество дубликатов (< 1%)
3. ✅ Данные за весь период существования маркета (с 19 ноября 2024)
4. ✅ Корректная работа progress callback с отображением скорости
5. ✅ Правильное определение даты начала (из API или fallback)

## Поддержка

Если возникли проблемы:
1. Проверьте логи на наличие ошибок
2. Убедитесь, что API доступен и rate limiting работает
3. Проверьте, что база данных инициализирована правильно
4. Убедитесь, что используется правильный `conditionId` (не numeric ID)

