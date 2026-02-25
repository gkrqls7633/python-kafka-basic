import asyncio
import json
from fastapi import FastAPI, BackgroundTasks
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager

# 1. 전역 변수로 Producer 선언 (싱글톤처럼 활용)
producer = None


# 2. Lifecycle 관리 (앱 시작시 Start, 종료시 Stop)
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    print("Kafka Producer 시작 중...")
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 자동 직렬화 설정
    )
    await producer.start()
    try:
        yield
    finally:
        print("Kafka Producer 종료 중...")
        await producer.stop()


app = FastAPI(lifespan=lifespan)


# 3. 메시지 전송 엔드포인트
@app.post("/send-message")
async def send_message(topic: str, data: dict):
    """
    비동기로 Kafka에 메시지를 전송합니다.
    """
    # send()는 내부 버퍼에 넣는 작업이라 매우 빠릅니다.
    # 결과를 기다리고 싶다면 await producer.send_and_wait()를 사용하세요.
    await producer.send(topic, data)

    return {"status": "Message sent to Kafka", "data": data}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)