import logging
import sys
from pathlib import Path


LOG_FILE = 'hh_search.log'
LOG_ENCODING = 'utf8'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

formatter = logging.Formatter(LOG_FORMAT)
handler = logging.FileHandler(LOG_FILE, mode='a', encoding=LOG_ENCODING)
handler.setFormatter(formatter)

logging.basicConfig(level=logging.DEBUG, handlers=[handler])
logger = logging.getLogger('hh_search')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False


import tenacity
import hydra
from hydra import compose, initialize
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlencode
import polars as pl
import pendulum
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm


hydra.initialize(version_base=None, config_path='conf')
conf = hydra.compose(config_name='config')

TELEGRAM_TOKEN = conf.telegram.token
TELEGRAM_CHAT_ID = conf.telegram.chat_id



def send_telegram_message(text: str):
    """Отправляет текстовое сообщение в Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token or chat_id not set, message not sent")
        return
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': text,
            'parse_mode': 'HTML'
        }
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
    except Exception as error:
        logger.error(f"Failed to send Telegram message: {error}")


def log_retry_attempt(retry_state):
    """Логирует неудачную попытку перед повторным запросом"""
    attempt_number = retry_state.attempt_number
    exception = retry_state.outcome.exception()
    sleep_seconds = retry_state.upcoming_sleep
    
    logger.warning(
        f'Попытка #{attempt_number} не удалась! '
        f'Ошибка: {exception} '
        f'Повтор через {sleep_seconds} секунд'
    )


def log_retry_start(retry_state):
    """Логирует начало новой попытки (начиная со второй)"""
    if retry_state.attempt_number >= 2:
        logger.warning(f'Начало попытки #{retry_state.attempt_number}')


def handle_final_failure(retry_state):
    """Обрабатывает ситуацию, когда все попытки запроса исчерпаны"""
    logger.error(
        f'Все {retry_state.attempt_number} попыток не удались. '
        f'Последняя ошибка: {retry_state.outcome.exception()}'
    )
    return retry_state.outcome.exception()

retry_config = tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_fixed(1),
    before=log_retry_start,
    before_sleep=log_retry_attempt,
    retry_error_callback=handle_final_failure,
    reraise=False
)


@retry_config
def fetch_vacancies_page(url: str) -> requests.Response:
    """
    Выполняет HTTP-запрос к hh.ru с повторными попытками при ошибках
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/13'
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response


def main():
   
    start_time = pendulum.now('UTC')
    total_vacancies_found = 0
    pages_processed = 0
    error_occurred = False
    error_message = ""

    try:
        
        db_engine = create_engine(conf.database.url)
        with db_engine.connect() as connection:
            query = text("SELECT MAX(request_dttm) AS hwm FROM etl.hh_search")
            result = connection.execute(query).fetchone()
            last_request_time = result[0] if result else None

        
        if last_request_time is None:
            date_from = '01.01.2026 00:00:00'
        else:
            date_from = pendulum.instance(last_request_time).format("DD.MM.YYYY HH:mm:ss")

        logger.info(f"Поиск вакансий с {date_from}")

        
        search_params = {
            'area': conf.hh_api.area,
            'text': conf.hh_api.text,
            'search_field': conf.hh_api.search_field,
            'date_from': date_from,
            'no_magic': 'true',
            'items_on_page': conf.hh_api.get('items_on_page', 20),
            'order_by': 'publication_time',
            'enable_snippets': 'true'
        }

       
        first_page_params = search_params.copy()
        first_page_params['page'] = 0
        first_page_url = f"https://hh.ru/search/vacancy?{urlencode(first_page_params)}"

        response = fetch_vacancies_page(first_page_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        initial_data = json.loads(soup.select('template#HH-Lux-InitialState')[0].text)

   
        paging_info = initial_data['vacancySearchResult']['paging']
        
        if paging_info is None:
            logger.info('Нет новых вакансий')
            total_pages = 0
        else:
            if paging_info['lastPage'] is None:
               
                total_pages = paging_info['pages'][-1]['page'] + 1
            else:
                total_pages = paging_info['lastPage']['page'] + 1

        
        for page_number in tqdm(range(total_pages), desc="Processing pages"):
            logger.info(f"Обработка страницы {page_number}")
            
            current_page_params = search_params.copy()
            current_page_params['page'] = page_number
            current_page_url = f"https://hh.ru/search/vacancy?{urlencode(current_page_params)}"
            
            request_time = pendulum.now('UTC')
            page_response = fetch_vacancies_page(current_page_url)
            page_soup = BeautifulSoup(page_response.content, 'html.parser')
            page_data = json.loads(page_soup.select('template#HH-Lux-InitialState')[0].text)
            
            vacancies_on_page = page_data['vacancySearchResult']['vacancies']
            parsed_rows = []
            
            for vacancy_item in vacancies_on_page:
                row = {
                    'request_dttm': request_time,
                    'vacancy_id': vacancy_item['vacancyId'],
                    'vacancy_title': vacancy_item['name'],
                    'company_id': vacancy_item['company']['id'],
                    'company_title': vacancy_item['company']['name'],
                    'company_visible_name': vacancy_item['company']['visibleName'],
                    'publication_time': pendulum.parse(vacancy_item['publicationTime']['$']),
                    'last_change_time': pendulum.parse(vacancy_item['lastChangeTime']['$']),
                    'creation_time': pendulum.parse(vacancy_item['creationTime']),
                    'is_adv': vacancy_item.get('@isAdv', 'false'),
                    'snippet': json.dumps(vacancy_item['snippet'], ensure_ascii=False),
                    'responses_count': vacancy_item['responsesCount'],
                    'total_responses_count': vacancy_item['totalResponsesCount']
                }
                parsed_rows.append(row)
            
            if parsed_rows:
                data_frame = pl.DataFrame(parsed_rows)
                data_frame.write_database('etl.hh_search', db_engine, if_table_exists='append')
                total_vacancies_found += len(parsed_rows)
                pages_processed += 1

        end_time = pendulum.now('UTC')
        duration = (end_time - start_time).in_words(locale='ru')
        
        report_message = (
            f"<b>Результаты поиска вакансий</b>\n"
            f"Начало поиска: {start_time}\n"
            f"Обработано страниц: {pages_processed}\n"
            f"Новых вакансий: {total_vacancies_found}\n"
            f"Время выполнения: {duration}"
        )

    except Exception as error:
        logger.exception("Critical error in main script")
        error_occurred = True
        error_message = str(error)
        report_message = f"<b>Ошибка при выполнении скрипта</b>\n\n{error_message}"
    
    finally:
        send_telegram_message(report_message)
        
        sys.exit(1 if error_occurred else 0)


if __name__ == '__main__':
    main()
