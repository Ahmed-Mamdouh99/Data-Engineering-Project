import pandas as pd
import os
import re



def clean_wh(df: pd.DataFrame)->pd.DataFrame:
  pass


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
    # Write dataset to file
    df_population.to_csv(pop_filename)
  else:
    # Load population dataset
    df_population = pd.read_csv(pop_filename)
  # Merge datasets
  df_renamed.drop('population', axis=1, inplace=True)
  df_renamed.set_index(['country', 'year'], inplace=True)
  df_population.set_index(['country', 'year'], inplace=True)
  df_renamed = df_renamed.join(df_population).reset_index()
  # Impute missing values
  df_renamed = df_renamed.fillna((df_renamed.median(axis=0)), inplace=True)
  return df_renamed


def clean_250_cnt(df: pd.DataFrame)->pd.DataFrame:
  pass


def merge_data(df_wrld_happi: pd.DataFrame, df_life_exp: pd.DataFrame, df_250_cnt: pd.DataFrame)->pd.DataFrame: # TODO: Implement this
  pass
