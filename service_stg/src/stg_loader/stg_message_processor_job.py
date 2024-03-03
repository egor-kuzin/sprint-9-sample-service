import time
from datetime import date, datetime
from logging import Logger
import json
from typing import Dict, Tuple

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

    def get_fields(self, message: Dict) -> Tuple[int, str, date, str]:
        if message is not None:
            object_id = message.get("object_id")
            object_type = message.get("object_type")
            sent_dttm = message.get("sent_dttm")
            payload = message.get("payload")
            
            self._logger.info(f"{datetime.utcnow()}: object_id: {object_id}")
            self._logger.info(f"{datetime.utcnow()}: object_type: {object_type}")
            self._logger.info(f"{datetime.utcnow()}: sent_dttm: {sent_dttm}")
            self._logger.info(f"{datetime.utcnow()}: payload: {payload}")
            
            if object_id is not None:
                try:
                    object_id = int(object_id)
                except ValueError:
                    self._logger.error(
                        f"{datetime.utcnow()}: object_id is not a number: {object_id}"
                    )
            if object_type is not None:
                object_type = str(object_type)
            if sent_dttm is not None:
                sent_dttm = datetime.strptime(sent_dttm, "%Y-%m-%d %H:%M:%S")
            if payload is not None:
                payload = json.dumps(payload)
            
        return object_id, object_type, sent_dttm, payload
    
    
    def insert_into_db(
        self, 
        object_id: int, 
        object_type: str, 
        sent_dttm: date, 
        payload: str
    ) -> None:
        if isinstance(object_id, int):
            self._stg_repository.order_events_insert(
                object_id=object_id,
                object_type=object_type, 
                sent_dttm=sent_dttm, 
                payload=payload
            )
            self._logger.info(
                f"{datetime.utcnow()}: Message inserted into DB."
            )
        else:
            self._logger.info(
                f"{datetime.utcnow()}: Message not inserted into DB: object_id is not integer."
            )

    
    def get_user_data(self, payload: str) -> Dict :
        if payload is not None:
            user_data = self._redis.get(
                json.loads(payload).get('user').get("id")
            )
            self._logger.info(f"{datetime.utcnow()}: User data retrieved from Redis.")
            
            return user_data


    def get_restaurant_data(self, payload: str) -> Dict :
        if payload is not None:
            restaurant_data = self._redis.get(
                json.loads(payload).get('restaurant').get("id")
            )
            self._logger.info(f"{datetime.utcnow()}: Restaurant data retrieved from Redis.")
            
            return restaurant_data
    
    
    def constract_message(
        self,
        object_id: int, 
        object_type: str, 
        payload: str,
        user_data: Dict,
        restaurant_data: Dict
    ) -> Dict:
        message = {}
        m_user = {}
        m_restaurant = {}
        m_products = []
        m_payload = {}
        
        # FIXME: hold None
        m_user['id'] = user_data.get('_id')
        m_user['name'] = user_data.get('name')
        
        m_restaurant['id'] = restaurant_data.get('_id')
        m_restaurant['name'] = restaurant_data.get('name')
        
        payload = json.loads(payload)
        for product in payload.get('order_items'):
            product_id = product.get('id')
            product_category = [p.get('category') for p in restaurant_data.get('menu') if p.get('_id') == product_id][0]
            m_products.append(
                {
                    'id': product_id, 
                    'price': product.get('price'),
                    'quantity': product.get('quantity'),
                    'name': product.get('name'),
                    'category': product_category
                    }
                )
            
        m_payload['id'] = object_id
        m_payload['date'] = payload.get('date')
        m_payload['cost'] = payload.get('cost')
        m_payload['payment'] = payload.get('payment')
        m_payload['status'] = payload.get('final_status')
        m_payload['products'] = m_products
        
        message['object_id'] = object_id
        message['object_type'] = object_type
        message['payload'] = m_payload
        
        self._logger.info(f"{datetime.utcnow()}: Message constructed.")
        
        return message
    
                    
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START...")
        
        for i in range(self._batch_size):
            # Get message from Kafka
            message = self._consumer.consume()
            self._logger.info(f"{datetime.utcnow()}: Message received.")
            
            # Parse fields from message
            object_id, object_type, sent_dttm, payload = self.get_fields(message=message)
            
            # Insert fields into DB if object_id is not None
            self.insert_into_db(object_id, object_type, sent_dttm, payload)
            
            # Get user data from Redis
            user_data = self.get_user_data(payload=payload)
            
            # Get restaurant data from Redis
            restaurant_data = self.get_restaurant_data(payload=payload)
            
            # Construct message for Kafka
            message_out = self.constract_message(
                object_id, 
                object_type, 
                payload, 
                user_data, 
                restaurant_data
            )
            
            # Send message to Kafka
            self._producer.produce(message_out)
            self._logger.info(f"{datetime.utcnow()}: Message sent.")
                                                                
        self._logger.info(f"{datetime.utcnow()}: FINISH.")
