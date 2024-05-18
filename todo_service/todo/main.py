from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from todo import setting
from sqlmodel import SQLModel
import json


class Order(SQLModel):
    order_id:int
    product_id: int
    product_name: str
    product_price: int


async def create_topic():
    admin_client = AIOKafkaAdminClient(bootstrap_servers=setting.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=setting.KAFKA_ORDER_TOPIC, num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{setting.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{setting.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()

@asynccontextmanager
async def lifespan (app:FastAPI):
    print("Fastapi app started...")
    await create_topic()
    yield


app = FastAPI(lifespan=lifespan,
              title="FastAPI Producer Service...",
              version= '1.0.0'
              )

@app.get('/')
async def root():
    return {"message":"Welcome to the todo microservice"}


@app.post('/create_order')
async def create_order(order:Order):
    # print(f"""Order from user: {order}, Type:{type(order)}""")
    producer = AIOKafkaProducer(bootstrap_servers=setting.BOOTSTRAP_SERVER)
    
    serialized_order = json.dumps(order.__dict__).encode('utf-8')
    # print(f"""Serialized {serialized_order}, Type:{type(serialized_order)}""")
    await producer.start()
    try:
        await producer.send_and_wait(setting.KAFKA_ORDER_TOPIC,serialized_order)
    finally:
        await producer.stop()
    
    return {"message": "Order generated successfully"}



async def consume_orders():    
    
    consumer = AIOKafkaConsumer(setting.KAFKA_ORDER_TOPIC,
                                bootstrap_servers=setting.BOOTSTRAP_SERVER,
                                group_id=setting.KAFKA_CONSUMER_GROUP_ID)
    
    await consumer.start()
    try:
        
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)

    finally:
        await consumer.stop()

    
# @app.get('/get_order')
# async def get_order(background_tasks: BackgroundTasks):
#     background_tasks.add_task(consume_orders)
#     return {"message": "Started consuming orders"}