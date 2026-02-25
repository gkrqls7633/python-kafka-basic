import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer

# 로깅 설정 (실무 필수)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def process_message(msg):
    """
    실제 비즈니스 로직이 들어가는 부분입니다.
    예: DB 저장, 외부 API 호출, 이미지 처리 등
    """
    try:
        data = msg.value
        logger.info(f"Processing: {data} (Offset: {msg.offset})")
        # 비즈니스 로직 시뮬레이션
        await asyncio.sleep(1)
        logger.info(f"Done: {data}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


async def consume():
    # 1. 컨슈머 인스턴스 생성
    consumer = AIOKafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        group_id="my-worker-group",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000
    )

    # 2. 컨슈머 시작
    await consumer.start()
    logger.info("✅ Consumer Worker started. Waiting for messages...")

    try:
        # 3. 무한 루프 수신
        async for msg in consumer:
            await process_message(msg)
    except asyncio.CancelledError:
        logger.info("Worker task is being cancelled...")
    finally:
        # 4. 종료 시 안전하게 연결 해제 (중요!)
        logger.info("Stopping consumer...")
        await consumer.stop()
        logger.info("Consumer stopped successfully.")


if __name__ == "__main__":
    # 프로세스 종료 시그널(Ctrl+C 등) 처리
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 실행
    try:
        loop.run_until_complete(consume())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
