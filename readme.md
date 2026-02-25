# 발행된 이벤트 확인
bash
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning


uvicorn kafka_basic:app --reload
./.venv/bin/python kafka_workers.py