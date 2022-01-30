# SETUP
from datetime import datetime
import geopy.distance
import glob
import io
import keyboard
import numpy as np
from os import listdir
import pandas as pd
import pickle
import random
import time




MAX_FRIENDS = 10
USERS_TOTAL = 100




# Inputs from dataframe
    # El resultado diario no puede ser mayor a 14.05 km
def get_distance_km(lat1, lat2, lon1, lon2):
    pos1 = (lat1, lon1)
    pos2 = (lat2, lon2)
    return geopy.distance.distance(pos1, pos2).km




def score_IMC(weight, height):
    IMC = weight / ((height / 100)**2)
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

# Putuación mínima posible: -11.4
# Puntuación máxima posible: 100
# La puntuación debería ser la media.

def get_score(df_0, walk, bike):
    # IMC -> weight: 7; height: 8.
    score = score_IMC(df_0[7], df_0[8])*0.2
    # Bodyfat -> age: 5; gender: 6; bodyfat: 9
    score += score_bodyfat(df_0[5], df_0[6], df_0[9])*0.1
    # Cholesterol: 12
    score += score_cholesterol(df_0[12])*0.1
    # Smoker: 13
    score += (-200*int(df_0[13]) + 100)*0.05
    # Drinking: 14
    score += score_drink(df_0[14])*0.05
    # Disability: 15
    score += (-200*int(df_0[15]) + 100)*0.05
    # Previous patology: 16
    score += (-200*int(df_0[16]) + 100)*0.05
    # km_walk: 1; km_bike: 2.
    score += score_km(walk, bike)*0.4
    score = 17.6277 + 0.823723*score
    return score




time_count = list()
# Iterable
iter_np = range(USERS_TOTAL)
df = np.array([]).reshape(0,21)

col_names = list()
list(map(lambda x: col_names.append('friend_' + str(x)), range(1,MAX_FRIENDS+1)))
df_friends = pd.DataFrame(columns = col_names)
k = 0

while True:
    try:
        # k+1 in order to avoid .gitignore (local case, maybe)
        with (open(f"data/{listdir('data')[k+1]}", "rb")) as f:
            temp_np = pickle.load(f)
            
    except:
        break

    start_time = time.time()
    # Añadir temp al dataframe
    # col_temp = list(list(users_generated.values())[0].keys())
    # col_temp.remove('position')
    # col_temp.append('lat')
    # col_temp.append('lon')
    df = np.concatenate([df, temp_np])
    

    # FIXME: it will replaced by Kafka instructions
    # Read will be other format.
    if k > 0:
        # Add values
        # print(f'Before km_walk and bike: {time.time() - start_time}')
        rt_np_walk = np.array(list(map(lambda x: get_distance_km(df[-2*USERS_TOTAL+x][-2], 
                                                                 df[-USERS_TOTAL+x][-2], 
                                                                 df[-2*USERS_TOTAL+x][-1], 
                                                                 df[-USERS_TOTAL+x][-1]) 
                                       if df[-2*USERS_TOTAL+x][4] == 'Walking' and df[-USERS_TOTAL+x][4] == 'Walking' 
                                       else 0, 
                                       iter_np)))
        rt_np_walk_total += rt_np_walk
        
        rt_np_bike = np.array(list(map(lambda x: get_distance_km(df[-2*USERS_TOTAL+x][-2], 
                                                                 df[-USERS_TOTAL+x][-2], 
                                                                 df[-2*USERS_TOTAL+x][-1], 
                                                                 df[-USERS_TOTAL+x][-1]) 
                                       if df[-2*USERS_TOTAL+x][4] == 'Bike' and df[-USERS_TOTAL+x][4] == 'Bike' 
                                       else 0, 
                                       iter_np)))
        rt_np_bike_total += rt_np_bike

        # print(f'Before score: {time.time() - start_time}')
        # First score registered
        if k == 1:
            rt_np_score = np.array(list(map(lambda x: get_score(temp_np[x], rt_np_walk[x], rt_np_bike[x]), 
                                            iter_np)))

        else:
            rt_np_score = np.array(list(map(lambda x: np.mean([get_score(temp_np[x], rt_np_walk[x], rt_np_bike[x]),
                                                               rt_np_score[x]]), 
                                            iter_np)))

        
    # First iteration
    else:
        temp_id = np.array(list(map(lambda x: x[0], temp_np)))
        rt_np_walk = np.zeros(temp_id.shape)
        rt_np_bike = np.zeros(temp_id.shape)
        rt_np_score = np.zeros(temp_id.shape)
        rt_np_walk_total = np.zeros(temp_id.shape)
        rt_np_bike_total = np.zeros(temp_id.shape)
        
        # Friends dataframe
        # FIXME: with Kafka running, it will be different.
        df_friends = pd.DataFrame(list(map(lambda x: temp_np[x][3], iter_np)), columns = col_names)
        df_friends.index = pd.Index(temp_id)
        # Save friends dataframe
        df_friends.to_pickle('output/users_friends.pkl')
    

    # Save dataframe as a pickle file
    #             df.tail(USERS_TOTAL).to_pickle('data/users_stats.pkl')
    with open('output/users_stats.pkl', 'wb') as f:
        np.save(f, temp_np, allow_pickle = True)
    # Save output
    #             df_real_time = pd.DataFrame(data = np.transpose([rt_np_walk, rt_np_bike, rt_np_score]), 
    #                                         columns = ['km_walk', 'km_bike', 'score'], 
    #                                         index = temp_id)
    #             df_real_time.to_pickle('data/users_real_time.pkl')
    with open('output/users_real_time.pkl', 'wb') as f:
        np.save(f, np.array([temp_id, rt_np_walk, rt_np_bike, rt_np_score]), allow_pickle = True)

    # Drop rows
    df = df[-2*USERS_TOTAL:]
    # Print execution time
    time_count.append(time.time() - start_time)
    k += 1
    print("--- %s seconds ---" % time_count[-1])
    # Wait
    time.sleep(2)


    #     except Exception as err:
    #         print(f"Unexpected {err}, {type(err)}")
    #         break

    # Not taking into account the first iteration.
    # print(f'Average execution time per iteration: {sum(time_count[1:])/(len(time_count)-1)}')
    # print(f'Number of iterations: {len(time_count)}')




def color_producer(val):
    if val >= 50:
        # Minimum value: yellow
        red = hex(round(510 - 5.1*val))[-2:].replace('x', '0')
        green = 'ff'
    else:
        # Max value: yellow
        red = 'ff'
        green = hex(round(5.1*val))[-2:].replace('x', '0')
    return('#' + red + green + '00')




import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster

my_map = folium.Map(
    location= (39.45, -0.35),
    zoom_start = 11.75,
    control_scale = True
)


# Add a bubble map to the base map
for i in range(0,len(rt_np_score)):
    folium.Circle(location = [temp_np[i][-2], temp_np[i][-1]], 
                  radius = 5,
                  color = color_producer(rt_np_score[i].item())).add_to(my_map)
# list(map(lambda i: folium.Circle(location = [temp_np[i][-2], temp_np[i][-1]], radius = 20, color = color_producer(rt_np_score[i].item()).add_to(my_map), range(len(rt_np_score))))
# folium.Circle(location = [temp_np[0][-2], temp_np[0][-1]], radius = 20, color = color_producer(rt_np_score[0])).add_to(my_map)

# Choropleth(geo_data=districts.__geo_interface__, 
#            data=plot_dict, 
#            key_on="feature.id", 
#            fill_color='YlGnBu', 
#            legend_name='Major criminal incidents (Jan-Aug 2018)'
#           ).add_to(m_6)

my_map



# FIXME
def json_to_geojson(data, districts):
    # create a geojson from a list of dictionaries
    # containing coordinates with the name of the polygon
    # in our case a polygon is a district
    assert type(data) == list, "The parameter data should be a list of coordinates with a name argument!"
    
    geojson = {
        "type": "FeatureCollection",
        "features": [
        {
            "type": "Feature",
            "geometry" : {
                "type": "Polygon",
                "name": district,
                "coordinates": [[[d["lon"], d["lat"]] for d in data if d['name'] == district]],
                },
            "properties" : {'name': district},
            
         } for district in districts]
    }
    
    return geojson

