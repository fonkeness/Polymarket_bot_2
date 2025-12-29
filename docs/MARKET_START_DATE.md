# Как получить дату начала маркета (Market Start Date)

## Обзор

Для получения даты начала маркета в Polymarket используется **Gamma API**. В коде уже реализована логика для получения этой информации.

## API Endpoint

**Gamma API:** `GET /markets/{numeric_id}`

- **Base URL:** `https://gamma-api.polymarket.com`
- **Требуется:** Numeric market ID (не condition_id!)

## Как получить дату начала маркета

### Шаг 1: Получить numeric market ID

Numeric ID можно получить из ответа `get_event_markets()`:

```python
from src.parser.api_client import PolymarketAPIClient

api_client = PolymarketAPIClient()
event_data = api_client.get_event_markets("event-slug")

# В ответе markets[].id - это numeric ID
for market in event_data.get("markets", []):
    numeric_id = market.get("id")  # Это numeric ID
    condition_id = market.get("conditionId")  # Это condition_id
```

### Шаг 2: Получить информацию о маркете

```python
from src.parser.api_client import PolymarketAPIClient

api_client = PolymarketAPIClient()
market_info = api_client.get_market_info(str(numeric_id))
```

### Шаг 3: Проверить поля с датой

API может возвращать дату начала в следующих полях (проверяются в указанном порядке):

1. `createdAt` - Unix timestamp (int) или ISO строка
2. `created_at` - Unix timestamp (int) или ISO строка  
3. `startDate` - Unix timestamp (int) или ISO строка
4. `start_date` - Unix timestamp (int) или ISO строка
5. `created` - Unix timestamp (int) или ISO строка

### Пример использования

```python
from src.parser.api_client import PolymarketAPIClient
from datetime import datetime, timezone

api_client = PolymarketAPIClient()

# Получить numeric ID из события
event_data = api_client.get_event_markets("epstein-client-list-released-in-2025-372")
first_market = event_data.get("markets", [])[0]
numeric_id = first_market.get("id")

# Получить информацию о маркете
market_info = api_client.get_market_info(str(numeric_id))

# Проверить поля с датой
date_fields = ["createdAt", "created_at", "startDate", "start_date", "created"]
start_timestamp = None

for field_name in date_fields:
    if field_name in market_info:
        field_value = market_info[field_name]
        
        if isinstance(field_value, (int, float)):
            start_timestamp = int(field_value)
        elif isinstance(field_value, str):
            # Попробовать распарсить ISO формат
            try:
                dt = datetime.fromisoformat(field_value.replace("Z", "+00:00"))
                start_timestamp = int(dt.timestamp())
            except ValueError:
                pass
        
        if start_timestamp:
            start_date = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            print(f"✓ Найдена дата начала: {field_name} = {start_date}")
            break

if start_timestamp:
    print(f"Дата начала маркета: {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)}")
else:
    print("⚠️  Дата начала не найдена в API ответе")
    print("Доступные поля:", list(market_info.keys()))
```

## Текущая реализация в коде

Логика получения даты начала уже реализована в `src/parser/trade_parser.py`:

```python
# Строки 330-355
start_timestamp = None

try:
    market_info = await api_client.get_market_info(condition_id)
    # Проверяет поля: createdAt, created_at, startDate, start_date, created
    for field_name in ["createdAt", "created_at", "startDate", "start_date", "created"]:
        if field_name in market_info:
            # ... обработка ...
except Exception:
    # Использует fallback дату
    pass
```

**Важно:** В текущей реализации есть проблема - `get_market_info()` вызывается с `condition_id`, но метод требует `numeric_id`. Это может не работать для всех маркетов.

## Fallback стратегия

Если API не возвращает дату начала, используется fallback:

1. **Если в БД есть трейды:** `min_timestamp_in_db - 1 день`
2. **Если БД пустая:** Фиксированная дата `2024-11-19` или `current_date - 1 год`

## Тестирование

Для проверки, какие поля возвращает API, можно использовать скрипт:

```bash
python scripts/test_market_info_fields.py
```

Этот скрипт:
- Проверяет ответ API для маркета
- Выводит все доступные поля
- Ищет поля, связанные с датами
- Показывает структуру ответа

## Документация API

Официальная документация Polymarket API:
- **Gamma API:** `https://gamma-api.polymarket.com`
- **Data API:** `https://data-api.polymarket.com`

**Примечание:** Polymarket не предоставляет публичную документацию API. Информация получена из анализа кода и ответов API.

## Рекомендации

1. **Всегда проверяйте numeric ID:** Убедитесь, что используете правильный numeric ID, а не condition_id
2. **Обрабатывайте ошибки:** API может не вернуть дату для всех маркетов
3. **Используйте fallback:** Всегда имейте резервную стратегию определения даты начала
4. **Логируйте результаты:** Записывайте, какие поля были найдены для отладки

## Пример полного решения

```python
async def get_market_start_date(condition_id: str, api_client: AsyncPolymarketAPIClient) -> int | None:
    """
    Получить дату начала маркета из API.
    
    Args:
        condition_id: Market conditionId
        api_client: API клиент
        
    Returns:
        Unix timestamp даты начала или None
    """
    # Проблема: get_market_info требует numeric_id, а не condition_id
    # Нужно сначала получить numeric_id из события
    
    # Вариант 1: Попробовать использовать condition_id напрямую (может не работать)
    try:
        market_info = await api_client.get_market_info(condition_id)
        date_fields = ["createdAt", "created_at", "startDate", "start_date", "created"]
        
        for field_name in date_fields:
            if field_name in market_info:
                field_value = market_info[field_name]
                if isinstance(field_value, (int, float)):
                    return int(field_value)
                elif isinstance(field_value, str):
                    try:
                        dt = datetime.fromisoformat(field_value.replace("Z", "+00:00"))
                        return int(dt.timestamp())
                    except ValueError:
                        pass
    except Exception:
        pass
    
    # Вариант 2: Получить numeric_id из события (более надежно)
    # Для этого нужно знать event_slug или получить его из condition_id
    # Это требует дополнительной логики
    
    return None
```

