# NO TOCAR
from math import sin, cos, sqrt, atan2, radians
from faker import Faker
import keyboard
import json
import pandas as pd
import time
import random
from datetime import datetime

faker = Faker('es_ES')
USERS_TOTAL=100
users={}
lat_min=39.4
lat_max=39.5
lon_min=-0.3
lon_max=-0.4
vehicles=["Bike","Train","Car", "Walking"]

def initiate_data():
    global users
    for i in range(0,USERS_TOTAL):
        user={}
        user["id"]=faker.ssn()
        user["name"]=faker.first_name()
        user["last_name"]=faker.last_name()
        user["friends"]=[]
        user["position"]={"lat":random.uniform(39.4, 39.5),"lon":random.uniform(-0.3, -0.4)}
        user["transport"]=random.choice(vehicles)
        users[user["id"]]=user   
    num=0
    for element in users.items():
        print(f"Generating friends of {num} of {len(users)}")
        for i in range(0,random.randint(1,10)):
            friend=random.choice(list(users.values()))
            if friend["id"]!=element[0]:
                users[element[0]]["friends"].append(friend["id"])
            else:
                print("No friend of yourself") 
        num=num+1

    print("DATA GENERATED")        


def generate_step():
    global users
    if len(users)>0:
        print("STEP")
        for element in users.items():
            lat=users[element[0]]["position"]["lat"]
            lon=users[element[0]]["position"]["lon"]
            users[element[0]]["position"]["lon"]=lon+random.uniform(0.001, 0.005)
            users[element[0]]["position"]["lat"]=lat+random.uniform(0.001, 0.005)
            if lat>lat_max or lat<lat_min:
                users[element[0]]["position"]["lat"]=random.uniform(39.4, 39.5)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["position"]["lon"]=random.uniform(-0.3, -0.4)
    else:
        initiate_data()
    return users

def get_distance_km(lat1, lat2, lon1, lon2):
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance


# NO TOCAR
# Users dataframe initialization
# FIXME: find the way to avoid memory management problems. Maybe, removing the first USERS_TOTAL rows when the number of iterations is 10 or 5.
df = pd.DataFrame()
# Users dataframe with kms and score
df_real_time = pd.DataFrame(columns = ['id', 'km_walk', 'km_bike', 'score'])

while True:
    try:  
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            users_generated=generate_step()
            # Place your code here
            print("code")
            with open('users_test.json', 'w') as f:
                json.dump(users_generated, f)
            time.sleep(2)
            temp = pd.read_json('users_test.json')
            temp = temp.T
            temp['lat'] = list(map(lambda x: x.get('lat'), list(temp.position)))
            temp['lon'] = list(map(lambda x: x.get('lon'), list(temp.position)))
            temp.drop(columns='position')
            df = df.append(temp)
            
            if len(df.loc[df.id == df.id.iloc[0]])>1:
                # Add values
                df_real_time['km_walk'] = list(map(lambda x: get_distance_km(df.loc[df.id == x].lat[-2], df.loc[df.id == x].lat[-1], df.loc[df.id == x].lon[-2], df.loc[df.id == x].lon[-1]) 
                                                   if df.loc[df.id == x].transport.iloc[-1] == 'Walking' and df.loc[df.id == x].transport.iloc[-2] == 'Walking'
                                                   else 0, 
                                                   list(df_real_time.id.unique()))) + df_real_time['km_walk']
                df_real_time['km_bike'] = list(map(lambda x: get_distance_km(df.loc[df.id == x].lat[-2], df.loc[df.id == x].lat[-1], df.loc[df.id == x].lon[-2], df.loc[df.id == x].lon[-1]) 
                                                   if df.loc[df.id == x].transport.iloc[-1] == 'Bike' and df.loc[df.id == x].transport.iloc[-2] == 'Bike'
                                                   else 0, 
                                                   list(df_real_time.id.unique()))) + df_real_time['km_bike']
            else:
                # Initialize dataframe
                df_real_time['id'] = list(df.id.unique())
                df_real_time['km_walk'] = 0
                df_real_time['km_bike'] = 0
                df_real_time['km_score'] = 0
            
            df.to_csv('users_stats.csv')
            df_real_time.to_csv('users_real_time.csv')

    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break
