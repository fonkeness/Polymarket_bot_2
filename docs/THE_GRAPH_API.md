# The Graph API - Документация и Решения

## Обзор

The Graph - это децентрализованный протокол для индексации и запроса данных из блокчейнов. Для Polymarket используется субграф для получения данных о сделках.

## Текущая Конфигурация

```python
THE_GRAPH_API_KEY = "839805a5fb864b40b2fa49bca0a4c38d"
THE_GRAPH_SUBGRAPH_ID = "Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"
THE_GRAPH_API_URL = f"https://gateway.thegraph.com/api/{THE_GRAPH_API_KEY}/subgraphs/id/{THE_GRAPH_SUBGRAPH_ID}"
```

## Типы Endpoints The Graph

### 1. Gateway API (текущий)
- **URL формат:** `https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}`
- **Тип:** Централизованный сервис
- **Проблемы:** Может иметь проблемы с индексерами ("bad indexers", "too far behind")

### 2. Network Endpoint (децентрализованная сеть)
- **URL формат:** `https://api.studio.thegraph.com/query/{SUBGRAPH_ID}/{SUBGRAPH_NAME}/{VERSION}`
- **Тип:** Децентрализованная сеть
- **Преимущества:** Более надежный, использует несколько индексеров

### 3. Hosted Service (устаревший)
- **Статус:** Deprecated, не рекомендуется использовать

## Проблема: Ошибки Индексеров

### Типичные ошибки:
```
bad indexers: {
  0x920fdeb00ee04dd72f62d8a8f80f13c82ef76c1e: Unavailable(too far behind),
  0xbdfb5ee5a2abf4fc7bb1bd1221067aef7f9de491: BadResponse(...),
  0xedca8740873152ff30a2696add66d1ab41882beb: Unavailable(too far behind)
}
```

### Причины:
1. Индексеры отстают от блокчейна
2. Индексеры временно недоступны
3. Проблемы с синхронизацией данных

## Решения

### Решение 1: Retry Logic (уже реализовано)
- Повторные попытки с экспоненциальной задержкой
- До 3 попыток с задержками: 1s, 2s, 4s
- Возвращает пустой список после всех попыток

### Решение 2: Использование Network Endpoint
Попробовать использовать децентрализованную сеть вместо Gateway:

```python
# Альтернативный URL для Network Endpoint
THE_GRAPH_NETWORK_URL = f"https://api.studio.thegraph.com/query/{SUBGRAPH_ID}/polymarket/{VERSION}"
```

**Проблема:** Нужно знать точное имя субграфа и версию, которые могут отличаться.

### Решение 3: Fallback на Polymarket Data API
Если The Graph недоступен, использовать альтернативный API:

```python
# Использовать Data API Polymarket как fallback
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com"
```

**Проблема:** Data API может иметь другую структуру данных и ограничения.

### Решение 4: Увеличение таймаутов и задержек
- Увеличить timeout для запросов
- Добавить более длительные задержки между запросами
- Использовать более консервативный rate limiting

## Рекомендации

1. **Текущее решение (Retry Logic):** Хорошо для временных проблем с индексерами
2. **Мониторинг:** Отслеживать частоту ошибок индексеров
3. **Fallback стратегия:** Рассмотреть использование альтернативного API при длительных проблемах
4. **Обновление конфигурации:** Проверить актуальность API ключа и субграф ID

## GraphQL Запрос

Текущий запрос для получения сделок:

```graphql
query GetTrades($marketId: String!, $first: Int!, $skip: Int!) {
    trades(
        first: $first
        skip: $skip
        where: { market: $marketId }
        orderBy: timestamp
        orderDirection: desc
    ) {
        id
        market {
            id
        }
        outcomeIndex
        price
        amount
        timestamp
        user {
            id
        }
        side
    }
}
```

## Официальная Документация

- **The Graph Docs:** https://thegraph.com/docs/
- **Querying Subgraphs:** https://thegraph.com/docs/en/querying/querying-from-your-app/
- **Network Endpoints:** https://thegraph.com/docs/en/querying/querying-the-graph/

## Примечания

- Gateway API может иметь временные проблемы с индексерами
- Retry logic помогает справиться с временными проблемами
- Для критичных приложений рекомендуется использовать Network Endpoint или иметь fallback на альтернативный API

