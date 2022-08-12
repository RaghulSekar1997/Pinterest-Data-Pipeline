from ensurepip import bootstrap
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

# initialise instance of fastAPI
app = FastAPI()

# Pydantic class initialising different attributes to a specified data type
class Data(BaseModel):

    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

# Configure Producer which will send data to the KafkaPinterestTopic
pin_producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    client_id = "Pinterest_producer",
    # converts message sent to topic into bytes
    value_serializer = lambda pinmessage: dumps(pinmessage).encode("ascii")
)

# post requests to localhost:8000/pin/
@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    # publish message to the topic
    pin_producer.send(topic = "Pinterest_Topic", value = data)


if __name__ == '__main__':
    # use uvicorn to run api -> processes requests asynchronously
    uvicorn.run("kafka_producer:app", host="localhost", port=8000)