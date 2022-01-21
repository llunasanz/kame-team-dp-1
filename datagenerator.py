# NO TOCAR
from math import sin, cos, sqrt, atan2, radians
from faker import Faker
import keyboard
import json
import pandas as pd
# import numpy as np
import time
import random
from datetime import datetime
import geopy.distance
pd.set_option('display.max_columns', None)




faker = Faker('es_ES')
USERS_TOTAL=100
users={}
lat_min=39.4
lat_max=39.5
lon_min=-0.3
lon_max=-0.4
vehicles=["Bike","Train","Car", "Walking"]
# Radio de la Tierra aproximado en km
R = 6371.0




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
        user["age"]=random.uniform(16, 85)
        user["gender"]=random.choice(["man","woman"])
        user["weight"]=random.uniform(60, 110)
        user["height"]=random.uniform(150, 210)
        user["bodyfat"]=random.uniform(3, 45)
        user["bloodpressure_sist"]=random.uniform(120, 180)
        user["bloodpressure_diast"]=random.uniform(70, 120)
        user["cholesterol"]=random.uniform(150, 300)
        user["smoker"]=random.choice(["0","1"])
        user["drinking"]=random.uniform(0,7)
        user["disability"]=random.choice(["0","1"])
        user["previouspatology"]=random.choice(["0","1"])
        user["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        users[user["id"]]=user   
        print(user)
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
            users[element[0]]["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
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




# Inputs from dataframe
    # El resultado diario no puede ser mayor a 14.05 km
def get_distance_km(lat1, lat2, lon1, lon2):
    pos1 = (lat1, lon1)
    pos2 = (lat2, lon2)
    return geopy.distance.distance(pos1, pos2).km




def score_IMC(df):
    IMC = df.weight[0]/((df.height[0]/100)**2)
    
    if IMC < 16:
        return 33
    elif IMC >= 16 and IMC < 18.5:
        return 66
    elif IMC >= 18.5 and IMC < 25:
        return 100
    elif IMC >= 25 and IMC < 30:
        return 66
    else:
        return 33




def age_bodyfat(age, gender):
    if age < 30:
        if gender == 'male':
            return [11, 14, 21, 23]
        else:
            return [16, 20, 29, 31]
    elif age >= 30 and age < 40:
        if gender == 'male':
            return [12, 15, 22, 24]
        else:
            return [17, 21, 30, 32]
    elif age >= 40 and age < 50:
        if gender == 'male':
            return [14, 17, 24, 26]
        else:
            return [18, 22, 31, 33]
    else:
        if gender == 'male':
            return [14, 17, 24, 26]
        else:
            return [19, 23, 32, 34]
            




def score_bodyfat(df):
    age = df.age[0]
    gender = df.gender[0]
    bodyfat = df.bodyfat[0]
    if bodyfat < age_bodyfat(age, gender)[0]:
        return 100
    elif bodyfat >= age_bodyfat(age, gender)[0] and bodyfat < age_bodyfat(age, gender)[1]:
        return 80
    elif bodyfat >= age_bodyfat(age, gender)[1] and bodyfat < age_bodyfat(age, gender)[2]:
        return 60
    elif bodyfat >= age_bodyfat(age, gender)[2] and bodyfat < age_bodyfat(age, gender)[3]:
        return 40
    else:
        return 20




def score_cholesterol(df):
    cholesterol = df.cholesterol[0]
    if cholesterol < 200:
        return 100
    elif cholesterol >= 200 and cholesterol < 240:
        return 50
    else:
        return -100




def score_drink(df):
    drinking = df.drinking[0]
    if drinking < 0.5:
        return 100
    elif drinking >= 0.5 and drinking < 2:
        return 80
    elif drinking >= 2 and drinking < 4:
        return 40
    else:
        return -100




# df_1
def score_km(df):
    km_walk = df.km_walk.values
    km_bike = df.km_bike.values
    # La persona andó ese día
    if km_walk > 0 and km_walk < 8:
        return km_walk*12.5
    elif km_walk >= 8:
        return 100
    # Los que han ido en bici, tren o coche
    else:
        if km_bike > 0:
            return km_bike*3.6
        else:
            return 0




# Get the score for each user
    # df_0: temp.loc[temp.id == x].iloc[-1]
    # df_1: real time dataframe
    # FIXME: buscar la manera de que, para números distintos de 0, no devuelva una lista.
def get_score(df_0, df_1):
    score = score_IMC(df_0)*0.2     + score_bodyfat(df_0)*0.1     + score_cholesterol(df_0)*0.1     + (-200*df.smoker[0] + 100)*0.05     + score_drink(df_0)*0.05     + (-200*df.disability[0] + 100)*0.05     + (-200*df.previouspatology[0] + 100)*0.05     + score_km(df_1)*0.4
    if isinstance(score, float):
        return score
    else:
        return score[0]




df = pd.DataFrame()
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
            # Tipar variables de temp
            temp.age = temp.age.astype(float)
            temp.weight = temp.weight.astype(float)
            temp.height = temp.height.astype(float)
            temp.bodyfat = temp.bodyfat.astype(float)
            temp.cholesterol = temp.cholesterol.astype(float)
            temp.smoker = temp.smoker.astype(int)
            temp.drinking = temp.drinking.astype(float)
            temp.disability = temp.disability.astype(int)
            temp.previouspatology = temp.previouspatology.astype(int)
            
            # Añadir temp al dataframe
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
                df_real_time['score'] = list(map(lambda x: get_score(temp.loc[temp.id == x], df_real_time.loc[df_real_time.id == x]), 
                                                 list(df_real_time.id.unique()))) + df_real_time['score']
                
                
            else:
                # Initialize dataframe
                df_real_time['id'] = list(df.id.unique())
                df_real_time['km_walk'] = 0
                df_real_time['km_bike'] = 0
                df_real_time['score'] = 0
                df.to_csv('users_stats.csv')
                df_real_time.to_csv('users_real_time.csv')
            
            # Drop rows from df
            df = df.tail(USERS_TOTAL*10)
                
            
            
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break


