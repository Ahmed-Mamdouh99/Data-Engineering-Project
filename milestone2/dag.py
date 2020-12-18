from airflow import DAG
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import re
import os
import requests


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
  # Rename columns
  new_col_names = {}
  for col_name in df.columns:
    # Strip the name from extra white space and switch to lower case
    new_col_names[col_name] = re.sub(' +', ' ',col_name.strip().lower())
  df_renamed = df.rename(columns=new_col_names)
  # Drop infant deaths
  df_renamed.drop('infant deaths', axis=1, inplace=True)
  # Check if population data exists
  pop_filename = 'data/population-by-country.csv'
  df_population = None
  if not os.path.exists(pop_filename):
    # Download population data
    url = "https://pkgstore.datahub.io/JohnSnowLabs/population-figures-by-country/population-figures-by-country-csv_csv/data/630580e802a621887384f99527b68f59/population-figures-by-country-csv_csv.csv"
    os.system('curl "{}" --output {}'.format(url, pop_filename))
    df_population = pd.read_csv(pop_filename)
    # Tidy population dataset
    df_population = df_population.melt(id_vars=['Country', 'Country_Code'], var_name='year', value_name='population')
    df_population = df_population.rename(columns={'Country':'country', 'Country_Code':'country_code'})
    df_population['year'] = df_population['year'].str.extract(r'(\d+)').astype('int')
    df_population = df_population[(df_population['year'] >= 2000) & (df_population['year'] <= 2015)]
    updated_values = {
      "Saint Lucia": "LCA","Slovakia": "SVK",
      "Iran (Islamic Republic of)": "IRN",
      "Gambia": "GMB","Micronesia (Federated States of)": "FSM",
      "United States of America": "USA","Bahamas": "BHS",
      "Lao People's Democratic Republic": "LAO","Republic of Moldova": "MDA",
      "Cook Islands": "COK","Saint Vincent and the Grenadines": "VCT","Yemen": "YEM",
      "United Kingdom of Great Britain and Northern Ireland": "GBR","Côte d'Ivoire": "CIV",
      "Niue": "NIU","United Republic of Tanzania": "TZA","Saint Kitts and Nevis": "KNA",
      "Bolivia (Plurinational State of)": "BOL","Congo": "COG",
      "Venezuela (Bolivarian Republic of)": "VEN","Republic of Korea": "KOR",
      "Democratic People's Republic of Korea": "PRK","Egypt": "EGY","Viet Nam": "VNM",
      "Kyrgyzstan": "KGZ","Czechia": "CZE","Democratic Republic of the Congo": "COD",
      "The former Yugoslav republic of Macedonia": "MKD"
    }
    for name, code in updated_values.items():
      mask = df_population['country_code'] == code
      df_population.loc[mask, 'country'] = name
    df_population.reset_index(inplace=True, drop=True)
      # Write dataset to file
    df_population.to_csv(pop_filename)
  else:
    # Load population dataset
    df_population = pd.read_csv(pop_filename, index_col=0)
  # Merge datasets
  df_renamed.drop('population', axis=1, inplace=True)
  df_renamed = df_renamed.merge(df_population, on=['year', 'country'])
  # Impute missing values
  df_renamed.fillna((df_renamed.median(axis=0)), inplace=True)
  df_New = df_renamed.groupby(['country', 'country_code']).mean().reset_index()
  return df_New

def clean_250_cnt(countryData: pd.DataFrame)->pd.DataFrame:
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




# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2020, 12, 13)
  }


# step 3 - instantiate DAG
dag = DAG(
  'world-happiness-DAG',
  default_args=default_args,
  description='World Hapiness DAG',
  schedule_interval='@once',
)

# step 4 Define tasks
def store_data(**context):
  df = context['task_instance'].xcom_pull(task_ids='translate_data')
  df.to_csv("data/output.csv")


def translate_data(**context):
  [df_wrld_happi, df_250_cnt, df_life_exp] = context['task_instance'].xcom_pull(task_ids='extract_data')
  df_wrld_happi_cleaned = clean_wh(*df_wrld_happi)
  df_250_cnt_cleaned = clean_250_cnt(df_250_cnt)
  df_life_exp_cleaned = clean_life_exp(df_life_exp)
  df_merged = merge_data(df_wrld_happi_cleaned, df_life_exp_cleaned, df_250_cnt_cleaned)
  return df_merged


def extract_data(**kwargs):
  df_wrld_happi = []
  for i in range(15, 20):
    df_wrld_happi.append(pd.read_csv('data/df_wrld_happi_{}.csv'.format(i)))
  df_life_exp = pd.read_csv('data/df_life_exp.csv')
  df_250_cnt = pd.read_csv('data/df_250_cnt.csv')
  return [df_wrld_happi, df_250_cnt, df_life_exp]


t1 = PythonOperator(
  task_id='extract_data',
  provide_context=True,
  python_callable=extract_data,
  dag=dag,
)

t2 = PythonOperator(
  task_id='translate_data',
  provide_context=True,
  python_callable=translate_data,
  dag=dag,
)

t3 = PythonOperator(
  task_id='store_data',
  provide_context=True,
  python_callable=store_data,
  dag=dag,
)

# step 5 - define dependencies
t1 >> t2 >> t3
