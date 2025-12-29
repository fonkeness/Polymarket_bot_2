# Инструкции по тестированию Stage 3

## Что делает Stage 3

Stage 3 реализует оптимизированную async версию загрузчика трейдов с:
- **Async/await** для максимальной скорости
- **Offset-пагинация** для загрузки всех исторических данных
- **Real-time сохранение** в БД (батчами, не накапливая в памяти)
- **Rate limiting** (10 запросов в секунду)

## Предварительные требования

1. Убедитесь, что вы на сервере (SSH)
2. Активна виртуальная среда: `source venv/bin/activate`
3. Код обновлен: `git pull`

## Тестирование Stage 3

### 1. Быстрый тест (проверка что код работает)

```bash
cd ~/projects/Polymarket_bot_2
source venv/bin/activate
git pull

# Используйте conditionId из Stage 1 (или numeric ID, скрипт сам получит conditionId)
python scripts/fetch_all_trades.py "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01"
```

**Ожидаемый результат:**
- Скрипт получит conditionId (если передан numeric ID)
- Начнет загрузку с прогресс-баром: "Loaded 1000 trades...", "Loaded 2000 trades..." и т.д.
- После каждой тысячи трейдов - сохраняет в БД
- В конце выводит общее количество загруженных трейдов
- **Важно**: Должна быть загружена ВСЯ история (не только первые 500!)

### 2. Тест без сохранения в БД (только загрузка)

```bash
python scripts/fetch_all_trades.py "0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01" --no-db
```

**Ожидаемый результат:**
- Загружает трейды, но не сохраняет в БД
- Показывает прогресс загрузки
- Выводит общее количество загруженных трейдов

### 3. Проверка что Stage 2 все еще работает

```bash
# Используйте numeric ID (не conditionId)
python scripts/stage2_main.py "689356"
```

**Ожидаемый результат:**
- Должен работать точно так же, как и раньше
- Загружает первые 500 трейдов
- Сохраняет в БД
- Не должно быть ошибок

## Проверка результатов

### Проверка количества загруженных трейдов

После выполнения Stage 3, проверьте сколько трейдов в базе:

```bash
python -c "
from src.database.repository import get_trade_count
condition_id = '0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01'
count = get_trade_count(condition_id)
print(f'Total trades: {count}')
"
```

**Ожидаемый результат:**
- Должно быть значительно больше 500 трейдов (если они есть)
- Например, если Stage 2 загрузил 500, а Stage 3 загрузил всю историю, то должно быть намного больше

### Просмотр загруженных данных

```bash
python -c "
from src.database.repository import get_trades_by_market
from datetime import datetime

condition_id = '0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01'
trades = get_trades_by_market(condition_id, limit=10)

print('Last 10 trades:')
print(f'{'Date':<20} {'Price':<12} {'Size':<15}')
print('-' * 50)
for t in trades:
    date = datetime.fromtimestamp(t['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    print(f'{date:<20} {t[\"price\"]:<12.6f} {t[\"size\"]:<15.6f}')
"
```

## Возможные ошибки и их решение

### Ошибка 1: "ModuleNotFoundError: No module named 'asyncio'"

**Причина:** Python версия < 3.7 (asyncio встроен начиная с 3.4, но может быть проблема)

**Решение:** Проверьте версию Python: `python --version` (должна быть >= 3.7, лучше 3.12)

### Ошибка 2: "RuntimeError: This event loop is already running"

**Причина:** Конфликт event loop (редко, если запускаете из async контекста)

**Решение:** Убедитесь, что запускаете скрипт напрямую: `python scripts/fetch_all_trades.py`

### Ошибка 3: Rate limit ошибки (429 Too Many Requests)

**Причина:** Слишком много запросов

**Решение:** Rate limiting должен работать автоматически (10 req/sec), но если видите 429:
- Увеличьте задержку в `src/utils/config.py`: `API_RATE_LIMIT = 5.0` (вместо 10.0)

### Ошибка 4: Загружается только 1000 трейдов

**Причина:** Пагинация не работает правильно

**Проверка:**
- Убедитесь, что offset увеличивается: `offset += limit_per_page`
- Проверьте что цикл продолжается пока `len(trades_data) == limit_per_page`
- Проверьте логи - должна быть загрузка нескольких страниц

### Ошибка 5: "TypeError: 'coroutine' object is not iterable"

**Причина:** Использование async функции без await

**Решение:** Убедитесь, что `async_fetch_all_trades()` вызывается с `await` в async функции

## Сравнение Stage 2 vs Stage 3

| Параметр | Stage 2 | Stage 3 |
|----------|---------|---------|
| Метод | Синхронный | Async |
| Пагинация | Нет (только 500 трейдов) | Да (offset, все трейды) |
| Скорость | Медленнее | Быстрее (async) |
| Память | Накапливает все в памяти | Real-time saving (батчами) |
| Прогресс | Нет | Да (callback) |
| Скрипт | `stage2_main.py` | `fetch_all_trades.py` |

## Успешное тестирование

Stage 3 считается успешно протестированным, если:

1. ✅ Скрипт запускается без ошибок
2. ✅ Загружается больше трейдов, чем в Stage 2 (не только первые 500)
3. ✅ Прогресс-бар показывает загрузку батчей
4. ✅ Трейды сохраняются в БД по мере загрузки
5. ✅ Stage 2 все еще работает (обратная совместимость)
6. ✅ Нет ошибок rate limiting (429)
7. ✅ Данные корректны (можно проверить просмотром)

## Пример успешного вывода

```
============================================================
Fetch ALL Trades from Polymarket API (Async Mode)
============================================================
Market ID: 689356
------------------------------------------------------------
Initializing database...
Database initialized.

Getting conditionId...
Got conditionId: 0x0576b194302d7d0a3f7bfc1b843cef4a5c9d582a3f424f879c15441feed78f01

Starting to fetch all trades (this may take a while)...

Loaded 1000 trades...
Loaded 2000 trades...
Loaded 3000 trades...
Loaded 3427 trades...

============================================================
✓ Successfully fetched 3427 total trades
============================================================
✓ Total trades in database for market 0x0576...: 3427

✓ Done!
```

В этом примере загружено 3427 трейдов (вместо только 500 как в Stage 2) - это показывает, что пагинация работает правильно!

