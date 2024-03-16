from bs4 import BeautifulSoup
import requests
import pandas as pd


url = 'https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue'
page = requests.get(url)
soup = BeautifulSoup(page.text, "html.parser")

# Finding required table
table = soup.find('table', class_ = 'wikitable sortable')

# Fetching table headers
world_titles = table.find_all('th')
world_table_titles = [title.text.strip() for title in world_titles]

# Creating Dataframe 
df = pd.DataFrame(columns = world_table_titles)

# Fetching table rows
column_data = table.find_all('tr')
for row in column_data[1:]:
    row_data = row.find_all('td')
    individual_row_data = [data.text.strip() for data in row_data]
    
    length = len(df)
    df.loc[length] = individual_row_data

df.to_csv("Wiki_US_Companies_By_Revenue.csv", index=False)