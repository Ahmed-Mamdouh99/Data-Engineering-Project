import pandas as pd
import re
import numpy as np
import os
def clean_wh(data2015,data2016,data2017,data2018,data2019)->pd.DataFrame:
  data2015.drop(['Happiness Rank', 'Standard Error'], axis=1, inplace=True)
  data2015['Year'] = 2015
  data2016.drop(['Happiness Rank', 'Lower Confidence Interval', 'Upper Confidence Interval'], axis=1, inplace=True)
  data2016['Year'] = 2016
  data2017.drop(['Happiness.Rank', 'Whisker.high', 'Whisker.low'], axis=1, inplace=True)
  data2017.rename(columns={'Happiness.Score': 'Happiness Score', 'Economy..GDP.per.Capita.': 'Economy (GDP per Capita)', 
                          'Health..Life.Expectancy.': 'Health (Life Expectancy)', 'Trust..Government.Corruption.':'Trust (Government Corruption)', 'Dystopia.Residual': 'Dystopia Residual'}, inplace=True)
  data2017['Year'] = 2017
  data2018.drop(['Overall rank'], axis=1, inplace=True)
  data2018['Year'] = 2018
  data2019.drop(['Overall rank'], axis=1, inplace=True)
  data2019['Year'] = 2019
  data2015.replace({'Country':'Somaliland region'}, 'Somaliland Region',inplace=True)
  data2017.replace({'Country':'Taiwan Province of China'}, 'Taiwan',inplace=True)
  data2017.replace({'Country':'Hong Kong S.A.R., China'}, 'Hong Kong',inplace=True)
  data2018.replace({'Country or region':'Trinidad & Tobago'}, 'Trinidad and Tobago',inplace=True)
  data2019.replace({'Country or region':'Trinidad & Tobago'}, 'Trinidad and Tobago',inplace=True)
  data2019.replace({'Country or region':'Northern Cyprus'}, 'North Cyprus',inplace=True)

  #integration
  allYearsOld = pd.concat([data2015,data2016, data2017])
  allYearsOld.sort_values(['Country','Year'], inplace=True, ignore_index=True)
  allYearsNew = pd.concat([data2018, data2019])
  allYearsNew.sort_values(['Country or region', 'Year'], inplace=True, ignore_index=True)

  #feature
  allYearsNew['Dystopia Residual'] = allYearsNew['Score'] - allYearsNew['GDP per capita'] - allYearsNew['Social support'] - allYearsNew['Healthy life expectancy'] - allYearsNew['Freedom to make life choices'] - allYearsNew['Generosity'] - allYearsNew['Perceptions of corruption']

  allYearsOldAdjusted = allYearsOld.drop(['Region'], axis=1)
  allYearsNewAdjusted = allYearsNew.rename(columns={'Country or region': 'Country', 'Score': 'Happiness Score', 'GDP per capita':'Economy (GDP per Capita)', 'Healthy life expectancy':'Health (Life Expectancy)', 'Freedom to make life choices':'Freedom', 'Perceptions of corruption':'Trust (Government Corruption)', 'Social support':'Family'})
  allYearsCombined = pd.concat((allYearsOldAdjusted, allYearsNewAdjusted), axis=0)
  allYearsCombined.sort_values(['Country','Year'], inplace=True, ignore_index=True)
  allYearsCombined=allYearsCombined.dropna()
  return allYearsCombined


def clean_life_exp(df: pd.DataFrame)->pd.DataFrame:
  pass


def clean_250_cnt(df: pd.DataFrame)->pd.DataFrame:
  countryData['Real Growth Rating(%)'] = countryData['Real Growth Rating(%)'].str.extract('(\S+)%')[0].str.replace('–', '-').astype(float)
  countryData['Literacy Rate(%)'] = countryData['Literacy Rate(%)'].str.extract('(\S+)%')[0].str.replace('–', '-').astype(float)
  countryData['Inflation(%)'] = countryData['Inflation(%)'].str.extract('(\S+)%')[0].str.replace('–', '-').astype(float)
  countryData['Unemployement(%)'] = countryData['Unemployement(%)'].str.extract('(\d+\.?\d*)\D*%')[0].str.replace('–', '-').astype(float)
  countryData = countryData.drop(countryData[countryData['subregion'].isna()].index)
  indicies = countryData[countryData['area'].isna()]['area'].index 
  countryData.loc[indicies, 'area'] = [34.2, 83534, 1628, 1128, 374, 6220, 2512, 420, 3903, 61399]
  regionGroups = countryData.groupby(['region'])[['gini','Real Growth Rating(%)', 'Literacy Rate(%)', 'Inflation(%)', 'Unemployement(%)']].mean()
  for i in countryData['region'].unique():
    countryData.loc[(countryData['region']==i) & (countryData['gini'].isna()),'gini'] = regionGroups[regionGroups.index == i]['gini'].values[0]
    countryData.loc[(countryData['region']==i) & (countryData['Real Growth Rating(%)'].isna()),'Real Growth Rating(%)'] = regionGroups[regionGroups.index == i]['Real Growth Rating(%)'].values[0]
    countryData.loc[(countryData['region']==i) & (countryData['Literacy Rate(%)'].isna()),'Literacy Rate(%)'] = regionGroups[regionGroups.index == i]['Literacy Rate(%)'].values[0]
    countryData.loc[(countryData['region']==i) & (countryData['Inflation(%)'].isna()),'Inflation(%)'] = regionGroups[regionGroups.index == i]['Inflation(%)'].values[0]
    countryData.loc[(countryData['region']==i) & (countryData['Unemployement(%)'].isna()),'Unemployement(%)'] = regionGroups[regionGroups.index == i]['Unemployement(%)'].values[0]

  #feature 
  countryData['Population Density'] = countryData['population'] / countryData['area']
  countryData['Literate Count'] = countryData['population'] * (countryData['Literacy Rate(%)']/100)
  countryData['Illiterate Count'] = countryData['population'] - countryData['Literate Count']
  countryData['Unemployment Count'] = countryData['population'] * (countryData['Unemployement(%)']/100)
  countryData['Employment Count'] = countryData['population'] - countryData['Unemployment Count']
  countryData['Literate Working Rate'] = countryData['Employment Count'] / countryData['Literate Count']
  return countryData

def merge_data(df_wrld_happi: pd.DataFrame, df_life_exp: pd.DataFrame, df_250_cnt: pd.DataFrame)->pd.DataFrame: # TODO: Implement this
  worldHappines = df_wrld_happi[df_wrld_happi['Year']==2015].copy()
	worldHappines.drop('Year',axis=1,inplace=True)

	df_life_exp.rename(columns = {"country":"Country", "year":"Year"}, inplace=True)
	df_250_cnt.rename(columns = {"name":"Country"}, inplace=True)

	before = ['Somaliland region', 'Bolivia (Plurinational State of)','Congo (Brazzaville)','Congo (Kinshasa)','Iran (Islamic Republic of)', "Lao People's Democratic Republic", "Democratic People's Republic of Korea", 'Republic of Moldova', 'Russian Federation', 'Somaliland Region', 'Syrian Arab Republic', 'United Kingdom of Great Britain and Northern Ireland', 'United States of America', 'Venezuela (Bolivarian Republic of)', 'Viet Nam', 'Czechia']
	after = ['Somaliland Region', 'Bolivia', 'Congo', 'Democratic Republic of the Congo', 'Iran', "Laos", "Republic of Korea", 'Moldova', 'Russia', 'Somalia', 'Syria', 'United Kingdom', 'United States', 'Venezuela', 'Vietnam', 'Czech Republic']
	worldHappines['Country'].replace(before, after,inplace=True)
	df_life_exp['Country'].replace(before, after,inplace=True)
	worldAndLife = worldHappines.merge(df_life_exp, on=['Country'])

	before = ['Bolivia', 'Congo (Democratic Republic of the)', 'Iran (Islamic Republic of)', "Korea (Democratic People's Republic of)", 'Moldova (Republic of)', 'Russian Federation', 'Syrian Arab Republic', 'United Kingdom of Great Britain and Northern Ireland', 'United States of America', 'Venezuela (Bolivarian Republic of)', 'Viet Nam']
	after = ['Bolivia (Plurinational State of)', 'Democratic Republic of the Congo', 'Iran', 'Korea (Republic of)', 'Moldova', 'Russia', 'Syria', 'United Kingdom', 'United States', 'Venezuela', 'Vietnam']
	worldAndLife['Country'].replace(before, after,inplace=True)
	df_250_cnt['Country'].replace(before, after,inplace=True)
	allDataMerged = worldAndLife.merge(df_250_cnt, on='Country')
	return allDataMerged
