# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 17:36:21 2019

@author: balmai
"""

'Select a country (Capital english)'
'################################################'

country = 'FRANCE'

'External packages'
'################################################'

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
import datetime

sns.set()

import warnings

warnings.filterwarnings("ignore")

import os
import shutil
import numpy as np

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

'Set Directories'
'################################################'

working_directory = 'C:\\Users\\Ismail\\OneDrive\\Code\\Github\\Python\\ClimaTact'
data_path = working_directory + '\\DataCompressed'
decompressed_data_path = working_directory + '\\DataDecompressed'
countries_path = data_path + '\\country-list.txt'
ids_path = data_path + '\\isd-history.txt'
results_path = working_directory + '\\Results\\'
country_results_path = results_path + country + '\\'

os.chdir(working_directory)

'Internal packages'
'################################################'

import Services.DataEdit as dataedit
import Services.Zip as zip

'Create result folders'
'################################################'

if 'Results' not in os.listdir(working_directory):
    os.mkdir(results_path)

if country not in os.listdir(results_path):
    os.mkdir(country_results_path)
    os.mkdir(country_results_path + '\\National')

if 'Last5Years' not in os.listdir(country_results_path):
    os.mkdir(country_results_path + '\\Last5Years')
    os.mkdir(country_results_path + '\\Last5Years\\Top5')

'Read country and station id'
'################################################'

countries = dataedit.read_country_code(countries_path)
stations = dataedit.read_station_id(ids_path, countries)

country_station_dico = stations[["USAF", "STATION NAME"]].loc[stations["CTRY"] == country].to_dict('index')
country_stations = {country_station_dico[key]['USAF']: country_station_dico[key]['STATION NAME'] for key in
                    country_station_dico}

'Big ass result dataframe'
'################################################'

result_df = pd.DataFrame(index=country_stations.values(),
                         columns=['2050HottestDay', '2050MaxTemp', '2050Temp', '2050MinTemp', '2050ColdestDay'
                             , 'HottestDayRise', 'MaxTempRise', 'TempRise', 'MinTempRise', 'ColdestDayRise'
                             , 'HistHottestDay', 'HistColdestDay', 'HistMaxNbDaysOver40', 'HistMaxNbDaysUnder0'
                             , '2050Rain', '2050RainyDays', '2050MaxRain'
                             , '2050Humidex', '2050HighestHumidex', 'HumidexRise', 'HighestHumidexRise'
                             , 'RainRise', 'RainyDaysRise', 'MaxRainRise', 'HistMaxRainInADay'
                             , 'StartTemp', 'EndTemp', 'StartRain', 'EndRain'], dtype=float)

'Decompress data if not decompressed'
'################################################'

start_year = 1980
last_year = 2019
for year in range(start_year, last_year + 1):
    path = data_path + '\\' + str(year) + '.tar'
    zip.decompress(path, decompressed_data_path + '\\' + str(year), stations['USAF'], year)

'Analyse data for each station'
'################################################'

for station_id in country_stations:
    station_name = country_stations[station_id]

    try:
        df = dataedit.get_station_data(station_id, decompressed_data_path)
    except:
        print('Insufficient data for ' + station_name)
        if station_name in result_df.index:
            result_df = result_df.drop([station_name], axis=0)
        continue

    print('Now studying ' + station_name)

    try:
        yearly_data, yearly_std = dataedit.yearly_sampling(df)

        result_df['HistHottestDay'].at[station_name] = df['MaxTemp'].max()
        result_df['HistColdestDay'].at[station_name] = df['MinTemp'].min()
        result_df['HistMaxRainInADay'].at[station_name] = df['Rain'].max() / 365.25
        result_df['HistMaxNbDaysOver40'].at[station_name] = yearly_data['NbDaysOver40'].max()
        result_df['HistMaxNbDaysUnder0'].at[station_name] = yearly_data['NbDaysUnder0'].max()

        begin_date = '1980'

        ## Temp study

        period_begin, period_end = datetime.date(1980, 1, 1), datetime.date(2019, 12, 31)
        period_begin = max(period_begin, yearly_data[begin_date].index[0])
        result_df['StartTemp'].at[station_name] = period_begin
        result_df['EndTemp'].at[station_name] = period_end
        temp_df = yearly_data[period_begin:period_end][['Temp', 'MaxTemp', 'MinTemp', 'HottestDay', 'ColdestDay']]

        sns.lineplot(data=temp_df, palette="tab10", linewidth=2.5)
        plt.savefig(country_results_path + station_name + ' ' + 'Temperatures.jpg')
        plt.tight_layout()
        plt.clf()

        for col_to_study in temp_df.columns:
            X_train = temp_df.index.map(lambda x: x.year).values.reshape((-1, 1))
            Y_train = temp_df[col_to_study].values.reshape((-1, 1))

            model = LinearRegression().fit(X_train, Y_train)

            target = np.array([2050]).reshape(-1, 1)
            today = np.array([2020]).reshape(-1, 1)
            result_df['2050' + col_to_study].at[station_name] = model.predict(target)[0, 0]
            result_df[col_to_study + 'Rise'].at[station_name] = result_df['2050' + col_to_study].at[station_name] - \
                                                                model.predict(today)[0, 0]

        fig = plt.gcf()
        fig.set_size_inches(20, 13)
        sns.lineplot(
            data=df.resample('W').mean()[period_end - datetime.timedelta(5 * 365):period_end][['MinTemp', 'MaxTemp']],
            palette="tab10", linewidth=2.5)
        plt.savefig(country_results_path + '\\Last5Years\\' + station_name + ' ' + 'Temp.jpg')
        plt.tight_layout()
        plt.clf()

        ## Humidex study

        humidex_df = yearly_data[period_begin:period_end][['Humidex', 'HighestHumidex']]

        sns.lineplot(data=humidex_df[['Humidex', 'HighestHumidex']], palette="tab10", linewidth=2.5)
        plt.savefig(country_results_path + station_name + ' ' + 'Humidex.jpg')
        plt.tight_layout()
        plt.clf()

        for col_to_study in humidex_df.columns:
            X_train = humidex_df.index.map(lambda x: x.year).values.reshape((-1, 1))
            Y_train = humidex_df[col_to_study].values.reshape((-1, 1))

            model = LinearRegression().fit(X_train, Y_train)

            target = np.array([2050]).reshape(-1, 1)
            today = np.array([2020]).reshape(-1, 1)
            result_df['2050' + col_to_study].at[station_name] = model.predict(target)[0, 0]
            result_df[col_to_study + 'Rise'].at[station_name] = result_df['2050' + col_to_study].at[station_name] - \
                                                                model.predict(today)[0, 0]

        ## Rain study

        period_begin, period_end = datetime.date(1980, 1, 1), datetime.date(2019, 12, 31)
        period_begin = max(period_begin, yearly_data[begin_date].index[0])
        result_df['StartRain'].at[station_name] = period_begin
        result_df['EndRain'].at[station_name] = period_end
        rain_df = yearly_data[period_begin:period_end][['Rain', 'RainyDays', 'MaxRain']]

        sns.lineplot(data=rain_df[['Rain', 'MaxRain']], palette="tab10", linewidth=2.5)
        plt.savefig(country_results_path + station_name + ' ' + 'Rain.jpg')
        plt.tight_layout()
        plt.clf()

        sns.barplot(x=rain_df.index.map(lambda x: x.year), y=rain_df['RainyDays'], palette="Blues_d")
        plt.savefig(country_results_path + station_name + ' ' + 'RainyDays.jpg')
        plt.tight_layout()
        plt.clf()

        for col_to_study in rain_df.columns:
            X_train = rain_df.index.map(lambda x: x.year).values.reshape((-1, 1))
            Y_train = rain_df[col_to_study].values.reshape((-1, 1))

            model = LinearRegression().fit(X_train, Y_train)

            target = np.array([2050]).reshape(-1, 1)
            today = np.array([2020]).reshape(-1, 1)
            result_df['2050' + col_to_study].at[station_name] = model.predict(target)[0, 0]
            result_df[col_to_study + 'Rise'].at[station_name] = result_df['2050' + col_to_study].at[station_name] - \
                                                                model.predict(today)[0, 0]

        print('Finished study for ' + station_name)

    except:
        print('Failed study for ' + station_name)
        result_df = result_df.drop([station_name], axis=0)
        continue

print('Finished stations study for ' + country)

'Compare station data'
'################################################'

for col in result_df.drop(['StartTemp', 'EndTemp', 'StartRain', 'EndRain'], axis=1):
    if 'Rain' in col:
        sns_palette = 'PuBuGn_d'
    else:
        sns_palette = 'Reds'
    sorted_df = result_df.sort_values(by=[col], ascending=False)
    fig = plt.gcf()
    fig.set_size_inches(12, 12)
    sns.barplot(x=sorted_df[col], y=sorted_df.index, palette=sns_palette)
    plt.tight_layout()
    plt.savefig(country_results_path + '\\National\\' + col + 'ByCity.jpg')
    plt.clf()

'Plot a temperature map'
'################################################'

result_df['LAT'] = result_df.index.map(lambda x: stations.loc[stations['STATION NAME'] == x]['LAT'].iloc[0])
result_df['LON'] = result_df.index.map(lambda x: stations.loc[stations['STATION NAME'] == x]['LON'].iloc[0])
result_df['ELEV(M)'] = result_df.index.map(lambda x: stations.loc[stations['STATION NAME'] == x]['ELEV(M)'].iloc[0])

low_lon = result_df['LON'].min() - 0.5
high_lon = result_df['LON'].max() + 0.5
low_lat = result_df['LAT'].min() - 0.5
high_lat = result_df['LAT'].max() + 0.5

fig = plt.gcf()

ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([low_lon, high_lon, low_lat, high_lat], crs=ccrs.PlateCarree())

ax.coastlines("10m")
ax.add_feature(cfeature.BORDERS, linestyle=':')

ax.set_xticks([low_lon, low_lon + 0.33 * (high_lon - low_lon), low_lon + 0.66 * (high_lon - low_lon), high_lon],
              crs=ccrs.PlateCarree())
ax.set_yticks([low_lat, low_lat + 0.33 * (high_lat - low_lat), low_lat + 0.66 * (high_lat - low_lat), high_lat],
              crs=ccrs.PlateCarree())
lon_formatter = LongitudeFormatter(zero_direction_label=True)
lat_formatter = LatitudeFormatter()
ax.xaxis.set_major_formatter(lon_formatter)
ax.yaxis.set_major_formatter(lat_formatter)

result_df.plot(ax=ax, kind="scatter", x="LON", y="LAT",
               s=result_df['HottestDayRise'] * 50, label="Rise",
               c="2050HottestDay", cmap=plt.get_cmap("jet"),
               colorbar=True, alpha=1.0, figsize=(15, 15)
               )

plt.tight_layout()
plt.savefig(country_results_path + '\\National\\' + 'Map.jpg')
plt.clf()

'Plot a humidex map'
'################################################'

fig = plt.gcf()

ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([low_lon, high_lon, low_lat, high_lat], crs=ccrs.PlateCarree())

ax.coastlines("10m")
ax.add_feature(cfeature.BORDERS, linestyle=':')

ax.set_xticks([low_lon, low_lon + 0.33 * (high_lon - low_lon), low_lon + 0.66 * (high_lon - low_lon), high_lon],
              crs=ccrs.PlateCarree())
ax.set_yticks([low_lat, low_lat + 0.33 * (high_lat - low_lat), low_lat + 0.66 * (high_lat - low_lat), high_lat],
              crs=ccrs.PlateCarree())
lon_formatter = LongitudeFormatter(zero_direction_label=True)
lat_formatter = LatitudeFormatter()
ax.xaxis.set_major_formatter(lon_formatter)
ax.yaxis.set_major_formatter(lat_formatter)

result_df.plot(ax=ax, kind="scatter", x="LON", y="LAT",
               s=result_df['HumidexRise'] * 50, label="HumidexRise",
               c="2050HighestHumidex", cmap=plt.get_cmap("jet"),
               colorbar=True, alpha=1.0, figsize=(15, 15)
               )

plt.tight_layout()
plt.savefig(country_results_path + '\\National\\' + 'HumidexMap.jpg')
plt.clf()

'Plot a rain map'
'################################################'

fig = plt.gcf()

ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([low_lon, high_lon, low_lat, high_lat], crs=ccrs.PlateCarree())

ax.coastlines("10m")
ax.add_feature(cfeature.BORDERS, linestyle=':')

ax.set_xticks([low_lon, low_lon + 0.33 * (high_lon - low_lon), low_lon + 0.66 * (high_lon - low_lon), high_lon],
              crs=ccrs.PlateCarree())
ax.set_yticks([low_lat, low_lat + 0.33 * (high_lat - low_lat), low_lat + 0.66 * (high_lat - low_lat), high_lat],
              crs=ccrs.PlateCarree())
lon_formatter = LongitudeFormatter(zero_direction_label=True)
lat_formatter = LatitudeFormatter()
ax.xaxis.set_major_formatter(lon_formatter)
ax.yaxis.set_major_formatter(lat_formatter)

result_df.plot(ax=ax, kind="scatter", x="LON", y="LAT",
               s=result_df['2050Rain'], label="Rain",
               c="2050Rain", cmap=plt.get_cmap("RdBu"),
               colorbar=True, alpha=1.0, figsize=(15, 15)
               )

plt.tight_layout()
plt.savefig(country_results_path + '\\National\\' + 'MapRain.jpg')
plt.clf()

'Plot station names on a map'
'################################################'

fig = plt.gcf()
fig.set_figheight(25)
fig.set_figwidth(25)

ax = plt.axes(projection=ccrs.PlateCarree())
ax.set_extent([low_lon, high_lon, low_lat, high_lat], crs=ccrs.PlateCarree())

ax.coastlines("10m")
ax.add_feature(cfeature.BORDERS, linestyle=':')

ax.set_xticks([low_lon, low_lon + 0.33 * (high_lon - low_lon), low_lon + 0.66 * (high_lon - low_lon), high_lon],
              crs=ccrs.PlateCarree())
ax.set_yticks([low_lat, low_lat + 0.33 * (high_lat - low_lat), low_lat + 0.66 * (high_lat - low_lat), high_lat],
              crs=ccrs.PlateCarree())
lon_formatter = LongitudeFormatter(zero_direction_label=True)
lat_formatter = LatitudeFormatter()
ax.xaxis.set_major_formatter(lon_formatter)
ax.yaxis.set_major_formatter(lat_formatter)

for city in result_df.index:
    plt.text(result_df["LON"].at[city], result_df["LAT"].at[city], city)

plt.tight_layout()
plt.savefig(country_results_path + '\\National\\' + 'Names.jpg')
plt.clf()

'Save five hottest cities result aside'
'################################################'

top5 = result_df['HottestDayRise'].sort_values(ascending=False).index[0:5]

for city in top5:
    shutil.move(country_results_path + '\\Last5Years\\' + city + ' ' + 'Temp.jpg',
                country_results_path + '\\Last5Years\\Top5\\' + city + ' ' + 'Temp.jpg')

'Celebrate'
'################################################'

print('Finished national study for ' + country)
