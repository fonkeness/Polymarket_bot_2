# Как найти адрес контракта Polymarket CLOB

## О Polymarket CLOB

Согласно [статье на Habr](https://habr.com/ru/companies/metalamp/articles/851892/), Polymarket состоит из трех основных модулей:

1. **CTF** (Conditional Token Framework) от Gnosis - управляет токенами исходов
2. **CLOB** (Central Limit Order Book) - **собственная разработка Polymarket** для реализации ордербука и лимитных ордеров
3. **UMA** - децентрализованный оракул для разрешения споров

**CLOB** - это контракт, который отвечает за:
- Реализацию ордербука (order book)
- Лимитные ордера
- Агрегацию ликвидности
- События `OrderFilled` при заполнении ордеров

## Проблема
Транзакция `0xa08bf5e2acca8cf3f85209795cf278128a30869c5a8bfb97851e19fd21d0d21e` не найдена в сети Polygon.

## Возможные причины
1. Транзакция находится в другой сети (Ethereum вместо Polygon)
2. Хеш транзакции неверный или транзакция не существует
3. Транзакция была в тестовой сети

## Способы найти адрес контракта

### Способ 1: Через Polygonscan (рекомендуется)

1. Откройте https://polygonscan.com
2. В поиске введите "Polymarket CLOB" или "CLOB"
3. Найдите контракт с активностью (много транзакций)
4. Проверьте события контракта - должны быть OrderFilled события
5. Скопируйте адрес контракта

### Способ 2: Через GitHub репозитории Polymarket

CLOB - это собственная разработка Polymarket, поэтому адрес контракта может быть в их репозиториях:

1. Проверьте официальные GitHub репозитории Polymarket:
   - Поищите репозитории с названиями типа "polymarket-clob", "clob-contracts", "polymarket-contracts"
   - Проверьте файлы конфигурации (config.json, addresses.json, deployments/)
   - Посмотрите документацию в README

2. Проверьте официальную документацию Polymarket:
   - https://docs.polymarket.com (если существует)
   - Техническая документация для разработчиков

3. Проверьте их Discord/Telegram каналы для разработчиков

### Способ 3: Через известные адреса

Попробуйте найти адрес через:
- Блокчейн-эксплореры (Polygonscan)
- Сообщества разработчиков
- Документацию DeFi проектов

### Способ 4: Проверка транзакции на Ethereum

Если транзакция на Ethereum:
1. Откройте https://etherscan.io/tx/0xa08bf5e2acca8cf3f85209795cf278128a30869c5a8bfb97851e19fd21d0d21e
2. Проверьте, существует ли транзакция
3. Если да, то контракт может быть на Ethereum, а не на Polygon

### Способ 5: Поиск через активность на Polygonscan

1. Найдите недавнюю транзакцию на Polymarket через их интерфейс
2. Откройте транзакцию на Polygonscan
3. Посмотрите, какой контракт вызван (должен быть CLOB контракт)
4. Проверьте события этого контракта - должны быть `OrderFilled` события
5. Скопируйте адрес контракта

### Способ 6: Поиск через события OrderFilled

1. На Polygonscan перейдите в раздел "Logs" или "Events"
2. Используйте фильтр событий для поиска `OrderFilled`
3. Найдите контракт с большим количеством таких событий
4. Это и будет CLOB контракт Polymarket

## После нахождения адреса

1. Добавьте адрес в `src/utils/config.py`:
   ```python
   POLYMARKET_CLOB_CONTRACT_ADDRESS = "0x..."
   ```

2. Проверьте адрес:
   ```bash
   python scripts/find_polymarket_contract.py
   ```

3. Если адрес верный, запустите Phase 3:
   ```bash
   python scripts/stage3_blockchain.py <market_id>
   ```

## Альтернатива: Использование API

Если не удается найти адрес контракта, можно продолжать использовать REST API (как в Phase 2), который не требует адреса контракта:

```bash
python scripts/stage2_main.py <market_id>
```

## Полезные ссылки

- [Статья о Polymarket на Habr](https://habr.com/ru/companies/metalamp/articles/851892/) - подробный разбор архитектуры
- [Polygonscan](https://polygonscan.com) - блокчейн-эксплорер для Polygon
- [Polymarket API](https://clob.polymarket.com) - REST API Polymarket

