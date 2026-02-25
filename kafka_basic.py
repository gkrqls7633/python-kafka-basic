import asyncio
import json
from abc import ABC, abstractmethod
from typing import Dict
from fastapi import FastAPI, Depends
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager

# ==========================================
# 1. Domain & Port Layer (추상화 계층)
# ==========================================
class EventPublisher(ABC):
    """
    DIP 핵심: 고수준 모듈은 이 인터페이스에 의존합니다.
    Kafka인지, RabbitMQ인지, 단순 로그인지 알 필요가 없습니다.
    """
    @abstractmethod
    async def publish(self, topic: str, message: Dict):
        pass


# ==========================================
# 2. Infrastructure Layer (구현 계층 - Adapter)
# ==========================================
class KafkaEventPublisher(EventPublisher):
    """
    실제 Kafka 라이브러리(aiokafka)를 사용하는 구현체입니다.
    """
    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    async def start(self):
        await self._producer.start()

    async def stop(self):
        await self._producer.stop()

    async def publish(self, topic: str, message: Dict):
        # 실제 Kafka 전송 로직
        await self._producer.send(topic, message)


# ==========================================
# 3. Application Layer (Use Case)
# ==========================================
class SendMessageUseCase:
    """
    비즈니스 로직을 담당합니다.
    구체적인 Kafka 구현체가 아닌 EventPublisher 인터페이스에 의존합니다.
    """
    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher

    async def execute(self, topic: str, data: Dict):
        # 여기에 비즈니스 검증 로직이 들어갈 수 있습니다.
        if not topic:
            raise ValueError("Topic is required")
        
        # 데이터 가공 예시
        data['processed_by'] = "CleanArchitecture"
        
        # 인터페이스를 통해 전송
        await self.publisher.publish(topic, data)
        return {"status": "success", "data": data}


# ==========================================
# 4. Framework & Configuration (Wiring)
# ==========================================

# 전역 변수로 인스턴스 관리 (DI 컨테이너 역할 대용)
kafka_publisher = KafkaEventPublisher(bootstrap_servers='localhost:9092')

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("System Starting...")
    await kafka_publisher.start()
    yield
    print("System Shutting down...")
    await kafka_publisher.stop()

app = FastAPI(lifespan=lifespan)

# 의존성 주입 (Dependency Injection) 헬퍼
def get_use_case() -> SendMessageUseCase:
    # 여기서 구체적인 구현체(kafka_publisher)를 주입합니다.
    return SendMessageUseCase(publisher=kafka_publisher)

@app.post("/send-message")
async def send_message_endpoint(
    topic: str, 
    data: dict, 
    use_case: SendMessageUseCase = Depends(get_use_case)
):
    """
    엔드포인트는 Use Case만 실행하며, 내부 구현(Kafka)은 모릅니다.
    """
    return await use_case.execute(topic, data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)