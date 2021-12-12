# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 18:35:04 2019

@author: balmai
"""

import pandas as pd
import Services.Tooling as Tooling
import datetime
from os import listdir
from os.path import isfile, join


def get_data(complete_path):
    raw_df = pd.read_csv(complete_path, sep=',')

    df = raw_df[[' YEARMODA', '   TEMP', '   MAX  ', '  MIN  ', 'PRCP  ']]
    df.columns = ['Date', 'Temp', 'MaxTemp', 'MinTemp', 'Rain']
    df['Date'] = df['Date'].map(lambda x: pd.to_datetime(x, format="%Y%m%d"))
    df['Temp'] = df['Temp'].map(Tooling.convert_to_celsius)
    df['MaxTemp'] = df['MaxTemp'].map(Tooling.remove_asterix)
    df['MinTemp'] = df['MinTemp'].map(Tooling.remove_asterix)
    df['MaxTemp'] = df['MaxTemp'].map(Tooling.convert_to_celsius)
    df['MinTemp'] = df['MinTemp'].map(Tooling.convert_to_celsius)
    df['Rain'] = df['Rain'].map(Tooling.convert_rain)
    df['RainyDays'] = df['Rain'] > 0
    df['Over40'] = df['MaxTemp'] > 39.5
    df['Under0'] = df['MinTemp'] < 0.5
    df = df.set_index('Date')
    df = df.resample('D').mean()

    # print(df.loc[df['MaxTemp']>=40])
    # print('_'*40)

    return df


def yearly_sampling(df):
    yearly_data = df.resample('A').mean()
    yearly_std = df.resample('A').std()
    for col in yearly_data.columns:
        yearly_data['Std' + col] = yearly_std[col]
    yearly_data['HottestDay'] = df['MaxTemp'].resample('A').max()
    yearly_data['ColdestDay'] = df['MinTemp'].resample('A').min()
    yearly_data['HighestHumidex'] = df['Humidex'].resample('A').max()
    yearly_data['RainyDays'] = df['RainyDays'].resample('A').sum() / df['RainyDays'].resample('A').count() * 365.25
    yearly_data['MaxRain'] = df['Rain'].resample('A').max() / 365.25
    yearly_data['NbDaysOver40'] = df['Over40'].resample('A').sum()
    yearly_data['NbDaysUnder0'] = df['Under0'].resample('A').sum()
    # Tooling.remove_adjacent_nan_periods(yearly_data)
    return yearly_data, yearly_std


def read_country_code(path):
    data = pd.read_csv(path, sep="          ")
    dico = data.to_dict('index')
    return {key: dico[key][data.columns[0]] for key in dico}


def read_station_id(path, countries):
    data = pd.read_fwf(path)
    del data['WBAN']
    del data['ST']
    del data['CALL']
    data.dropna(inplace=True)
    data = data[4:]
    data = data.loc[data['CTRY'] != 'RI']
    data = data.loc[data['CTRY'] != 'MJ']
    data = data.loc[data['CTRY'] != 'AE']
    data = data.loc[data['CTRY'] != 'OD']
    data = data.loc[data['BEGIN'] > 10000000]
    data = data.loc[data['END'] > 10000000]
    data['BEGIN'] = data['BEGIN'].map(lambda x: pd.to_datetime(x, format="%Y%m%d"))
    data['END'] = data['END'].map(lambda x: pd.to_datetime(x, format="%Y%m%d"))
    data = data.loc[data['BEGIN'] <= datetime.date(1980, 1, 1)]
    data = data.loc[data['END'] >= datetime.date(2019, 12, 10)]
    data['CTRY'] = data['CTRY'].map(lambda x: countries[x])
    data = data.loc[data['USAF'] != '999999']
    data['STATION NAME'] = data['STATION NAME'].map(lambda x: x.replace("\\", ""))
    data['STATION NAME'] = data['STATION NAME'].map(lambda x: x.replace("/", ""))
    return data


def get_station_data(station_id, decompressed_data_path):
    paths = [join(decompressed_data_path, f) for f in listdir(decompressed_data_path) if
             not isfile(join(decompressed_data_path, f))]

    def gen():
        for path in paths:
            file_name = station_id + '-99999-' + path[-4:] + '.op.gz'
            if file_name not in listdir(path):
                yield pd.DataFrame(columns=['YEARMODA', 'TEMP', 'DEWP', 'WDSP', 'MXSPD', 'MAX', 'MIN', 'PRCP'])
            yield pd.read_fwf(path + '\\' + file_name, compression='gzip',
                              usecols=['YEARMODA', 'TEMP', 'DEWP', 'WDSP', 'MXSPD', 'MAX', 'MIN', 'PRCP'],
                              colspecs=[(0, 6), (7, 12), (14, 22), (24, 30), (31, 33), (35, 41), (42, 44), (46, 52),
                                        (53, 55), (57, 63), (64, 66), (68, 73), (74, 76), (78, 83), (84, 86), (88, 93),
                                        (95, 100), (102, 108), (108, 109), (110, 116), (116, 117), (118, 123),
                                        (123, 124), (125, 130), (132, 138)])

    df = pd.concat(gen())
    df.columns = ['Date', 'Temp', 'DewPoint', 'WindSpeed', 'MaxWindSpeed', 'MaxTemp', 'MinTemp', 'Rain']
    df['Date'] = df['Date'].map(lambda x: pd.to_datetime(x, format="%Y%m%d"))
    df['Temp'] = df['Temp'].map(Tooling.convert_to_celsius)
    df['MaxTemp'] = df['MaxTemp'].map(Tooling.convert_to_celsius)
    df['MinTemp'] = df['MinTemp'].map(Tooling.convert_to_celsius)
    df['DewPoint'] = df['DewPoint'].map(Tooling.convert_to_celsius)
    df['Humidity'] = Tooling.compute_humidity(df['Temp'], df['DewPoint'])
    df['Humidex'] = Tooling.compute_humidex(df['Temp'], df['DewPoint'])
    df['WindSpeed'] = df['WindSpeed'].map(Tooling.convert_to_kilometers)
    df['MaxWindSpeed'] = df['MaxWindSpeed'].map(Tooling.convert_to_kilometers)
    df['Rain'] = df['Rain'].map(Tooling.convert_rain)
    df['RainyDays'] = df['Rain'] > 0
    df['Over40'] = df['MaxTemp'] > 39.5
    df['Under0'] = df['MinTemp'] < 0.5
    df = df.set_index('Date')
    df = df.resample('D').mean()
    df.fillna(method='ffill')
    df['HumidexZone'] = df['Humidex'].map(Tooling.compute_humidex_zone)

    return df
