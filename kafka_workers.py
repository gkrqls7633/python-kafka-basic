import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 개별 컨슈머 로직 (함수화)
# ==========================================

async def run_test_consumer():
    """ test-topic 전용 컨슈머 (Group A) """
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers='localhost:9092',
        group_id="test-group",  # 전용 그룹 ID
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logger.info(f"[TestGroup] Received: {msg.value}")
    finally:
        await consumer.stop()

async def run_payment_consumer():
    """ payment-topic 전용 컨슈머 (Group B) """
    consumer = AIOKafkaConsumer(
        "payment-topic",
        bootstrap_servers='localhost:9092',
        group_id="payment-group",  # 전용 그룹 ID
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logger.info(f"[PaymentGroup] 💰 Processing: {msg.value}")
    finally:
        await consumer.stop()

# ==========================================
# 2. 메인 실행부 (병렬 실행)
# ==========================================

async def main():
    # 두 개의 서로 다른 컨슈머 태스크를 동시에 실행합니다.
    logger.info("🚀 Starting multiple consumer groups...")
    await asyncio.gather(
        run_test_consumer(),
        run_payment_consumer()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Terminated by user")