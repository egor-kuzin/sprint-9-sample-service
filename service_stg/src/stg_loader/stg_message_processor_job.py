import time
from datetime import datetime
from logging import Logger
import json

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START...")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        for i in range(self._batch_size):
            message = self._consumer.consume()
            if message is not None:
                object_id = message.get("object_id")
                object_type = message.get("object_type")
                sent_dttm = message.get("sent_dttm")
                payload = message.get("payload")
                self._logger.info(f"{datetime.utcnow()}: object_id: {object_id}")
                self._logger.info(f"{datetime.utcnow()}: object_type: {object_type}")
                self._logger.info(f"{datetime.utcnow()}: sent_dttm: {sent_dttm}")
                self._logger.info(f"{datetime.utcnow()}: payload: {payload}")
                
                if payload is not None:
                    payload = json.dumps(payload)
                if sent_dttm is not None:
                    sent_dttm = datetime.strptime(sent_dttm, "%Y-%m-%d %H:%M:%S")
                if object_type is not None:
                    object_type = str(object_type)
                if object_id is not None:
                    try:
                        object_id = int(object_id)
                        self._stg_repository.order_events_insert(
                            object_id=object_id,
                            object_type=object_type, 
                            sent_dttm=sent_dttm, 
                            payload=payload
                        )
                        self._logger.info(f"{datetime.utcnow()}: Message inserted into DB.")
                    except ValueError:
                        self._logger.error(
                            f"{datetime.utcnow()}: Message not inserted into DB: object_id is not integer."
                        )
                                                                
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH.")
