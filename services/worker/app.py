import os
import asyncio
import logging

import redis.asyncio as redis
import phonenumbers
from phonenumbers import carrier, geocoder

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Подключение к Redis
# -----------------------------------------------------------------------------


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")


# -----------------------------------------------------------------------------
# Обработка одного номера (страна + оператор)
# -----------------------------------------------------------------------------

def parse_phone(phone: str) -> str:
    """
    Синхронно определяет страну и оператора по номеру через библиотеку phonenumbers.

    Номер может быть с '+' или без; возвращается строка вида "Country: Operator".
    Вызывается из asyncio.to_thread, чтобы не блокировать event loop.
    """
    plus_phone = phone if phone.startswith("+") else "+" + phone
    parsed = phonenumbers.parse(plus_phone, None)
    country = geocoder.country_name_for_number(parsed, "en")
    operator = carrier.name_for_number(parsed, "en")
    return f"{country}: {operator}"


async def process_one_phone(phone: str, semaphore: asyncio.Semaphore) -> tuple[str, str]:
    """
    Обрабатывает один номер в отдельном потоке (asyncio.to_thread).

    Парсинг phonenumbers — CPU-bound, поэтому выполняем в пуле потоков,
    чтобы не блокировать цикл событий и обрабатывать много номеров параллельно.
    Возвращает кортеж (номер, строка "страна: оператор") или (номер, "Error: ...")
    при исключении.
    """
    async with semaphore:
        try:
            result = await asyncio.to_thread(parse_phone, phone)
            return (phone, result)
        except Exception as e:
            logger.warning(f"Ошибка разбора номера {phone}: {e}")
            return (phone, f"Error: {e}")


# -----------------------------------------------------------------------------
# Основной цикл воркера
# -----------------------------------------------------------------------------

async def phone_service() -> None:
    """
    Бесконечный цикл: ожидание задачи из Redis, параллельная обработка номеров,
    запись результата и обновление статуса.

    Шаги:
    1. brpop(QUEUE_NAME) — блокирующее ожидание task_id.
    2. Статус задачи -> "processing".
    3. Сбор всех номеров из task:{task_id}:phones через hscan_iter.
    4. Параллельная обработка номеров (asyncio.gather + to_thread).
    5. Запись результатов в Redis одним pipeline.
    6. Статус задачи -> "processed".
    """
    # Ограничиваем количество потоков
    MAX_CONCURRENT_THREADS = os.cpu_count()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_THREADS)

    while True:
        res = await redis_client.brpop(QUEUE_NAME, 0)

        if not res:
            continue

        _queue_name, task_id = res
        logger.info(f"Processing task: {task_id}")

        # Статус в processing (по докстрингу)
        await redis_client.set(f"task:{task_id}:status", "processing")

        # Собираем все номера из хеша (итератор по полям, без полной загрузки в память)
        phones = []
        async for phone, _ in redis_client.hscan_iter(f"task:{task_id}:phones"):
            phones.append(phone)

        logger.info(f"Task {task_id}: processing {len(phones)} phone(s)")

        # Обрабатываем номера параллельно
        results = await asyncio.gather(
            *[process_one_phone(phone, semaphore=semaphore) for phone in phones],
            return_exceptions=True,
        )

        # Записываем результаты одним pipeline
        pipe = redis_client.pipeline()
        for result in results:
            # Проверяем, не является ли результат объектом исключения
            if isinstance(result, Exception):
                logger.error(f"Критическая ошибка при обработке номера: {result}")
                continue
            
            # Теперь распаковка безопасна
            phone, data = result
            pipe.hset(f"task:{task_id}:phones", phone, data)
            
        await pipe.execute()

        await redis_client.set(f"task:{task_id}:status", "processed")
        logger.info(f"Task {task_id}: completed, {len(phones)} number(s) processed")


# -----------------------------------------------------------------------------
# Точка входа
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        logger.info("Starting phone service worker")
        asyncio.run(phone_service())
    except KeyboardInterrupt:
        logger.info("Service STOP")