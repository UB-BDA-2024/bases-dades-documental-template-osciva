from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)   
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)


    # Get MongoDB database and collection
    database = mongo.getDatabase("data")
    collection = mongo.getCollection("sensors")
    
        # Prepare GeoJSON location
    location = {
        "type": "Point",
        "coordinates": [sensor.longitude, sensor.latitude]
    }

    # Create document
    doc = {
        "name": sensor.name,
        # "longitude": location["coordinates"][0],
        # "latitude": location["coordinates"][1],
        "location": location,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version
    }

    # Insert document into MongoDB collection
    collection.insert_one(doc)

    return db_sensor

def record_data(db: Session, redis: RedisClient, mongo: MongoDBClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    
    # Record sensor data in Redis
    temperature_key = f"sensor-{sensor_id}:temperature"
    if data.temperature is not None:
        redis.set(temperature_key, data.temperature)
    
    humidity_key = f"sensor-{sensor_id}:humidity"
    if data.humidity is not None:
        redis.set(humidity_key, data.humidity)

    battery_level_key = f"sensor-{sensor_id}:battery_level"
    redis.set(battery_level_key, data.battery_level)

    last_seen_key = f"sensor-{sensor_id}:last_seen"
    redis.set(last_seen_key, data.last_seen)

    velocity_key = f"sensor-{sensor_id}:velocity"
    if data.velocity is not None:
        redis.set(velocity_key, data.velocity)

    mongo_database = mongo.getDatabase("data")
    collection = mongo.getCollection("sensors")

    # Update sensor data in the database
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    documental_sensor = collection.find_one({"name": db_sensor.name})

    last_seen = redis.get(last_seen_key)
    battery_level = redis.get(battery_level_key)
    # temperature = None
    # humidity = None
    # velocity = None

    # if data.temperature is not None:
    #     temperature = redis.get(temperature_key)
    # if data.humidity is not None:
    #     humidity = redis.get(humidity_key)
    # if data.velocity is not None:
    #     velocity = redis.get(velocity_key)


    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        # latitude=documental_sensor["latitude"], # cambiar a [[[]]]
        # longitude=documental_sensor["longitude"],
        latitude = documental_sensor["location"]["coordinates"][1],
        longitude = documental_sensor["location"]["coordinates"][0],
        joined_at=str(db_sensor.joined_at),
        last_seen=last_seen,
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        # temperature=temperature,
        # humidity=humidity,
        # velocity=velocity
        temperature=data.temperature,
        humidity=data.humidity,
        velocity=data.velocity
    )



def get_data(db: Session, redis: RedisClient, mongo: MongoDBClient, sensor_id: int) -> schemas.Sensor:
    # Obtiene los datos del sensor de la base de datos
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # Obtiene los datos del sensor de Redis
    temperature_key = f"sensor-{sensor_id}:temperature"
    humidity_key = f"sensor-{sensor_id}:humidity"
    battery_level_key = f"sensor-{sensor_id}:battery_level"
    last_seen_key = f"sensor-{sensor_id}:last_seen"
    velocity_key = f"sensor-{sensor_id}:velocity"


    temperature = redis.get(temperature_key)
    humidity = redis.get(humidity_key)
    velocity = redis.get(velocity_key)
    battery_level = redis.get(battery_level_key)
    last_seen = redis.get(last_seen_key)
    
    mongo_database = mongo.getDatabase("data")
    collection = mongo.getCollection("sensors")
    documental_sensor = collection.find_one({"name": db_sensor.name})


    # Construye y retorna el objeto Sensor con los datos obtenidos
    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        # latitude=documental_sensor["latitude"], # cambiar a [[[]]]
        # longitude=documental_sensor["longitude"],
        latitude = documental_sensor["location"]["coordinates"][1],
        longitude = documental_sensor["location"]["coordinates"][0],
        joined_at=str(db_sensor.joined_at),
        last_seen=last_seen,
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        temperature=temperature,
        humidity=humidity,
        velocity=velocity
    )


def delete_sensor(db: Session, redis: RedisClient, mongo: MongoDBClient, sensor_id: int):
    # Obtiene el sensor de la base de datos
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # Elimina el sensor de la base de datos
    db.delete(db_sensor)
    db.commit()

    # Elimina los registros asociados en Redis
    temperature_key = f"sensor-{sensor_id}:temperature"
    humidity_key = f"sensor-{sensor_id}:humidity"
    velocity_key = f"sensor-{sensor_id}:velocity"
    battery_level_key = f"sensor-{sensor_id}:battery_level"
    last_seen_key = f"sensor-{sensor_id}:last_seen"

    redis.delete(temperature_key)
    redis.delete(humidity_key)
    redis.delete(velocity_key)
    redis.delete(battery_level_key)
    redis.delete(last_seen_key)

    database = mongo.getDatabase("data")

    # Elimina los registros asociados en MongoDB
    collection = mongo.getCollection("sensors")
    collection.delete_one({"name": db_sensor.name})

def get_sensors_near(db: Session, redis: RedisClient, mongo: MongoDBClient, latitude: float, longitude: float, radius: float) -> List[schemas.Sensor]:
    
    database = mongo.getDatabase("data")
    # Obtén la colección de MongoDB
    collection = mongo.getCollection("sensors")

    collection.create_index([("location", "2dsphere")])


    # Consulta los sensores cercanos utilizando la función $near
    query = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]  
                },
                "$maxDistance": radius
            }
        }
    }

    # Ejecuta la consulta en MongoDB
    nearby_sensors = list(collection.find(query))

    # Lista para almacenar los datos de los sensores cercanos
    nearby_sensor_data = []

    # Itera sobre los sensores cercanos
    for sensor_data in nearby_sensors:
        nom_sensor = sensor_data["name"]
        sensor = get_sensor_by_name(db, nom_sensor)
        sensor_datas = get_data(db=db, redis=redis, mongo=mongo, sensor_id=sensor.id)  
        nearby_sensor_data.append(sensor_datas)

    return nearby_sensor_data
