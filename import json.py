import json

# http://api.weatherapi.com/v1/current.json?key=&q=India&aqi=no
from datetime import datetime
import requests  
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("Weather")

def get_weather_data(city_id, api_key):  
    api_url = "http://api.openweathermap.org/data/2.5/weather"  # Corrected API URL
    params = {  
        "id": city_id,    
        "appid": api_key
    }  
    response = requests.get(api_url, params=params)  
    data = response.json()  
    return data

    
def lambda_handler(event, context):
    cities = {"Mobile": 4076598, "Dallas": 4684888, "Miami": 4164138, "Louisville": 4299276, "New Jersey": 5101760, 
              "Atlanta": 4180439, "Tampa": 4174757, "Memphis": 4641239, "Tempe": 5317058, "Edmond": 4535961}
    
    for city, city_id in cities.items():
        data = get_weather_data(city_id, "505a12d16d43c9ee02ae17fbe678a606")
        
        # Check if the data contains the expected keys
        if 'main' in data and 'temp' in data['main']:
            temp = data['main']['temp']
            wind_speed = data['wind']['speed']
            wind_dir = data['wind']['deg']
            pressure_mb = data['main']['pressure']
            humidity = data['main']['humidity']
            
            print(city, temp, wind_speed, wind_dir, pressure_mb, humidity)
            
            current_timestamp = datetime.utcnow().isoformat()
            item = {
                'City': city,
                'Time': str(current_timestamp),
                'temp': temp,
                'wind_speed': wind_speed,
                'wind_dir': wind_dir,
                'pressure_mb': pressure_mb,
                'humidity': humidity
            }
            
            item = json.loads(json.dumps(item), parse_float=Decimal)
            
            # Insert data into DynamoDB
            table.put_item(Item=item)
        else:
            print(f"Error: Weather data for {city} is not available.")



 