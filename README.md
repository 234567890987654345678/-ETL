# ETL-процесс для сбора вакансий с hh.ru

## Описание проекта

ETL-процесс для сбора данных о вакансиях с платформы **hh.ru**. Процесс реализует загрузку данных, сохраняет результаты в PostgreSQL, обеспечивает устойчивость к сбоям и отправляет уведомления в Telegram.


## Функционал

### ETL
Код реализует инкрементальную загрузку (запоминает максимальную дату предыдущего запроса и загружает только новые вакансии), выполняет HTTP-запросы к hh.ru с параметрами фильтрации, парсит HTML с извлечением JSON-данных с преобразованием дат в структурированные записи и сохраняет результат в PostgreSQL через `polars.write_database()`.

### Логирование
Логирование настроено на уровне DEBUG с записью в файл `hh_search.log`, фиксируются начало и завершение работы, количество обработанных страниц, найденные вакансии, ошибки и SQL-запросы через SQLAlchemy (уровень INFO).

### Обработка ошибок и ретраи
Обработка ошибок с помощью библиотеки `tenacity`: при сбое HTTP-запроса выполняется до 5 повторных попыток с интервалом в 1 секунду, логируются все неудачные попытки и финальная ошибка.

### Вынос конфигурации
Конфигурация вынесена в отдельный YAML-файл через фреймворк Hydra и содержит параметры поискового запроса, региона, поля поиска, строки подключения к базе данных и настроек Telegram.

## Настройка

### 1. Конфигурация `conf/config.yaml`

```yaml
hh_api:
  text: "системный аналитик"      
  area: 2                      # Санкт-Петербург
  search_field: "name"             

database:
  url: "postgresql+psycopg2://postgres:password@localhost:5432/your_db"

logging:
  level: "INFO"
  file: "tmp.text"

telegram:
  token: "token"    
  chat_id: "chat_id"          
```

### 2. Создание таблицы в PostgreSQL

```sql
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.hh_search (
    request_dttm      TIMESTAMP,
    vacancy_id        BIGINT,
    vacancy_title     TEXT,
    company_id        BIGINT,
    company_title     TEXT,
    company_visible_name TEXT,
    publication_time  TIMESTAMP,
    last_change_time  TIMESTAMP,
    creation_time     TIMESTAMP,
    is_adv            TEXT,
    snippet           TEXT,
    responses_count   INTEGER,
    total_responses_count INTEGER
);
```

### 3. Установка зависимостей

Установите необходимые библиотеки:

```bash
pip install requests beautifulsoup4 polars pendulum sqlalchemy psycopg2-binary hydra-core tenacity tqdm
```

## Запуск

```bash
python hh_search.py
```

## Логирование

Лог-файл `hh_search.log` содержит:
- Время начала и завершения работы
- Параметры поиска
- Количество обработанных страниц
- Количество найденных и сохранённых вакансий
- Ошибки с полным stack trace

## Уведомления в Telegram

По окончании работы (при отстутствии ошибок) приходит сообщение в Телеграм:

```
Результаты поиска вакансий
Начало поиска: 2026-03-29T12:00:00+00:00
Обработано страниц: 33
Новых вакансий: 650
Время выполнения: 1 минута 23 секунды
```
