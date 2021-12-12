# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 18:29:35 2019

@author: balmai
"""

import numpy as np


def convert_to_celsius(temp):
    if temp > 300 or temp < -300:
        return np.nan
    return (temp - 32.0) / 1.8


def convert_to_kilometers(miles):
    if miles > 900 or miles < 0:
        return np.nan
    return miles / 0.62137


def convert_inches_to_meters(inch):
    if inch > 999 or inch < 0:
        return 0
    return inch * 0.0254


def remove_asterix(temp):
    if temp[-1] == '*':
        return float(temp[:-1])
    return float(temp)


def convert_rain(rain):
    if float(rain) > 90:
        return np.nan
    return float(rain) * 25.4 * 365.25


def compute_humidity(temp, dew_point):
    return 100 * (np.exp((17.625 * dew_point) / (243.04 + dew_point)) / np.exp((17.625 * temp) / (243.04 + temp)))


def compute_humidex(temp, dew_point):
    return temp + 0.5555 * (6.11 * np.exp(5417.7530 * (1 / 273.16 - 1 / (273.15 + dew_point))) - 10)


def compute_humidex_zone(humidex):
    if humidex < 30:
        return 'None'
    if humidex < 35:
        return 'Slight'
    if humidex < 40:
        return 'Strong'
    if humidex < 45:
        return 'Severe'
    if humidex < 54:
        return 'Danger'
    return 'Death'


def remove_adjacent_nan_periods(df):
    to_remove = []
    to_remove.append(0)
    to_remove.append(len(df.index) - 1)
    for i in range(0, len(df.index)):
        if np.isnan(df.iloc[i].prod()):
            to_remove.append(i)
            to_remove.append(i - 1)
            to_remove.append(i + 1)
    for i in to_remove:
        df.iloc[i] = np.nan


def find_last_valid_period(df, col):
    if col != 'Rain':
        array = [bool(np.isnan(df[col].at[i]).prod()) for i in df.index]
    else:
        array = [bool(np.isnan(df[col].at[i]).prod()) or df[col].at[i] < 150 for i in df.index]
    array.reverse()
    index = array.index(False)
    last = len(array) - 1 - index
    index2 = array[index + 1:].index(True) + index
    first = len(array) - 1 - index2
    return df.index[first], df.index[last]
