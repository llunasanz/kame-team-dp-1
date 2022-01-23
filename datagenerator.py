#!/usr/bin/env python
# coding: utf-8



# SETUP
from datetime import datetime
from faker import Faker
import geopy.distance
import io
import keyboard
import numpy as np
import pandas as pd
import random
import time




faker = Faker('es_ES')
USERS_TOTAL=20000
MAX_FRIENDS=10
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
        user["age"]=random.uniform(16, 85)
        user["gender"]=random.choice(["man","woman"])
        user["weight"]=random.uniform(60, 110)
        user["height"]=random.uniform(150, 210)
        user["bodyfat"]=random.uniform(3, 45)
        user["bloodpressure_sist"]=random.uniform(90, 180)
        user["bloodpressure_diast"]=random.uniform(70, 120)
        user["cholesterol"]=random.uniform(150, 300)
        user["smoker"]=random.choice(["0","1"])
        user["drinking"]=random.uniform(0,7)
        user["disability"]=random.choice(["0","1"])
        user["previouspatology"]=random.choice(["0","1"])
        user["cp"]=random.randint(46001, 46025)
        user["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        users[user["id"]]=user   
        # print(user)
    num=0
    for element in users.items():
        # print(f"Generating friends of {num} of {len(users)}")
        for i in range(0,random.randint(1,MAX_FRIENDS)):
            friend=random.choice(list(users.values()))
            if friend["id"]!=element[0]:
                users[element[0]]["friends"].append(friend["id"])
            else:
                # print("No friend of yourself") 
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




def score_IMC(weight, height):
    IMC = weight/((height/100)**2)
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
            




def score_bodyfat(age, gender, bodyfat):
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




def score_cholesterol(cholesterol):
    if cholesterol < 200:
        return 100
    elif cholesterol >= 200 and cholesterol < 240:
        return 50
    else:
        return -100




def score_drink(drinking):
    if drinking < 0.5:
        return 100
    elif drinking >= 0.5 and drinking < 2:
        return 80
    elif drinking >= 2 and drinking < 4:
        return 40
    else:
        return -100




# df_1
def score_km(km_walk, km_bike):
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
    # x: id
    # df_0: temp.loc[x]
        # 12: cholesterol; 13: smoker; 14: disability; 15: previouspatology; 18: lat; 19: lon.
    # df_1: df_real_time.loc[df_real_time.id == x]
        # 1: km_walk; 2: km_bike
    
def get_score(df_0, df_1):
#     IMC -> weight: 7; height: 8.
    score = score_IMC(df_0[7], df_0[8])*0.2     # Bodyfat -> age: 5; gender: 6; bodyfat: 9
    + score_bodyfat(df_0[5], df_0[6], df_0[9])*0.1     # Cholesterol: 12
    + score_cholesterol(df_0[12])*0.1     # Smoker: 13
    + (-200*int(temp_np[0][13]) + 100)*0.05     # Drinking: 14
    + score_drink(df_0[14])*0.05     # Disability: 15
    + (-200*int(df_0[15]) + 100)*0.05     # Previous patology: 16
    + (-200*int(df_0[16]) + 100)*0.05     # km_walk: 1; km_bike: 2.
    + score_km(df_1[1], df_1[2])*0.4
    return score




df = pd.DataFrame()
col_names = list()
list(map(lambda x: col_names.append('friend_' + str(x)), range(1,MAX_FRIENDS+1)))
df_friends = pd.DataFrame(columns = col_names)
time_count = list()
# Iterable
iter_np = range(USERS_TOTAL)

while True:
    try:  
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            users_generated=generate_step()
            # The code goes here
            print("code")
            start_time = time.time()
            
            # NumPy array with the data
            temp_np = np.array(list(map(lambda v: list(list(users_generated.values())[v].values()), iter_np)))
            temp_np = np.array(list(map(lambda v: np.delete(np.append(temp_np[v], [temp_np[v][4].get('lat'), temp_np[v][4].get('lon')]), 4), iter_np)))
            temp_id = np.array(list(map(lambda x: x[0], temp_np)))
            temp_pos = np.array(list(map(lambda x: x[4], temp_np)))
            
            # Añadir temp al dataframe
            col_temp = list(list(users_generated.values())[0].keys())
            col_temp.remove('position')
            col_temp.append('lat')
            col_temp.append('lon')
            df = df.append(pd.DataFrame(temp_np, columns=col_temp).set_index('id'))
            df_np = np.array(df)
            
            if len(df) > USERS_TOTAL:
                # Add values
                # print(f'Before km_walk and bike: {time.time() - start_time}')
                rt_np_walk = list(map(lambda x: get_distance_km(df_np[-2*USERS_TOTAL+x][-2], 
                                                                df_np[-USERS_TOTAL+x][-2], 
                                                                df_np[-2*USERS_TOTAL+x][-1], 
                                                                df_np[-USERS_TOTAL+x][-1]) 
                                      if df_np[-2*USERS_TOTAL+x][3] == 'Walking' and df_np[-USERS_TOTAL+x][3] == 'Walking' 
                                      else 0, 
                                      iter_np)) \
                + rt_np_walk
                
                rt_np_bike = list(map(lambda x: get_distance_km(df_np[-2*USERS_TOTAL+x][-2], 
                                                                df_np[-USERS_TOTAL+x][-2], 
                                                                df_np[-2*USERS_TOTAL+x][-1], 
                                                                df_np[-USERS_TOTAL+x][-1]) 
                                      if df_np[-2*USERS_TOTAL+x][3] == 'Bike' and df_np[-USERS_TOTAL+x][3] == 'Bike' 
                                      else 0, 
                                      iter_np)) \
                + rt_np_bike
                
                # print(f'Before score: {time.time() - start_time}')
                rt_np_score = list(map(lambda x: get_score(temp_np[x], 
                                                           np.array(df_real_time.iloc[x])), 
                                       iter_np)) \
                + rt_np_score
                
            else:
                # Real time numpy arrays
                # temp_id will be used
                rt_np_walk = np.zeros(temp_id.shape)
                rt_np_bike = np.zeros(temp_id.shape)
                rt_np_score = np.zeros(temp_id.shape)
                
                # Friends dataframe
                df_friends = pd.DataFrame(list(map(lambda x: temp_np[x][3], iter_np)), columns = col_names)
                df_friends.index = pd.Index(temp_id)
                # Save friends dataframe
                df_friends.to_pickle('data/users_friends.pkl')
                
            # Save dataframe as a csv file
            df.to_pickle('data/users_stats.pkl')
            # Save output
            df_real_time = pd.DataFrame(data = np.transpose([rt_np_walk, rt_np_bike, rt_np_score]), 
                                        columns = ['km_walk', 'km_bike', 'score'], 
                                        index = temp_id)
            df_real_time.to_pickle('data/users_real_time.pkl')
            
            # Drop rows
            df = df.tail(USERS_TOTAL*4)
            # Print execution time
            time_count.append(time.time() - start_time)
            print("--- %s seconds ---" % time_count[-1])
            # Wait
            time.sleep(2)
            
            
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break

# Not taking into account the first iteration.
print(f'Average execution time per iteration: {sum(time_count[1:])/(len(time_count)-1)}')
print(f'Number of iterations: {len(time_count)}')

