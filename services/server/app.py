import os

from fastapi import FastAPI
import redis.asyncio as redis
import uuid

# -----------------------------------------------------------------------------
# Подключение к Redis / Приложение 
# -----------------------------------------------------------------------------


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

app = FastAPI()


# -----------------------------------------------------------------------------
# API: Полуцение номеров
# -----------------------------------------------------------------------------


QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")

@app.post("/process")
async def create_task(phones: list[str]):
    """
    Принимает список телефонов и создаёт задачу на проверку.

    В Redis сохраняются:
    - статус задачи (accepted),
    - хеш с номерами (значения пока 0),
    - task_id добавляется в очередь 'tasks' для воркера.

    Возвращает task_id — по нему потом запрашивают результат через GET /result.
    """
    task_id = str(uuid.uuid4())
    
    pipe = redis_client.pipeline()
    pipe.set(f"task:{task_id}:status", "accepted")
    for phone in phones:
        pipe.hset(f"task:{task_id}:phones", phone, None)
    pipe.lpush(QUEUE_NAME, task_id)
    await pipe.execute()

    return {"message": "Task created", "task_id": task_id}


# -----------------------------------------------------------------------------
# API: Возврат результата
# -----------------------------------------------------------------------------


@app.get("/result")
async def get_result(task_id: str):
    """
    Возвращает статус или результат задачи по task_id.

    - accepted / processing — задача ещё в работе, возвращается строка со статусом.
    - processed — результат (словарь номер -> "страна: оператор") возвращается клиенту,
      после чего данные задачи удаляются из Redis.
    - иначе — задача не найдена (неверный id или уже получена).
    """
    status = await redis_client.get(f"task:{task_id}:status")
    if status in ["accepted", "processing"]:
        return f"Task ID: {task_id} {status}"
    elif status == "processed":
        result = redis_client.hgetall(f"task:{task_id}:phones")
        await delete_task(task_id)
        return result
    else:
        return "Task not found"


async def delete_task(task_id: str):
    """
    Удаляет данные задачи из Redis после выдачи результата клиенту.

    Удаляются ключи task:{task_id}:phones и task:{task_id}:status.
    Выполняется одним round-trip через pipeline.
    """
    pipe = redis_client.pipeline()
    pipe.delete(f"task:{task_id}:phones")
    pipe.delete(f"task:{task_id}:status")
    await pipe.execute()