import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from sqlalchemy import create_engine

def Extract():
        all_results=[]

        for i in range(11665,12300):
                        url = f'https://trendshift.io/repositories/{i}'
                        resp = requests.get(url)
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        
                        list_row = [i]  # Start with the page number

                        # Extract data [Repo_Name]
                        name = soup.find_all('div', class_='flex items-center text-indigo-400 text-lg justify-between mb-1')
                        list_row.append(name[0].text if name else 'NoInfo')
                        
                        # Extract data [Github]
                        github_links = soup.find_all('a', class_='hover:cursor-pointer hover:underline')
                        list_row.append(github_links[0].get('href', 'NoInfo') if len(github_links) > 0 else 'NoInfo')
                        list_row.append(github_links[1].get('href', 'NoInfo') if len(github_links) > 1 else 'NoInfo')

                        # Extract data [Description]
                        description = soup.find_all('div', class_='text-sm text-gray-500')
                        list_row.append(description[0].text if description else 'NoInfo')

                        # Extract data [Language]
                        lang_names = soup.find_all('div', class_='text-gray-500 flex items-center text-xs md:text-sm')
                        list_row.extend([lang.text for lang in lang_names] if lang_names else ['NoInfo'])

                        # Extract data [Star and Fork]
                        star_fork_count = soup.find_all('div', class_='flex items-center')
                        list_row.append(star_fork_count[1].text.strip() if len(star_fork_count) > 1 else 'NoInfo')
                        list_row.append(star_fork_count[2].text.strip() if len(star_fork_count) > 2 else 'NoInfo')

                        # Extract data [Date]
                        createdate = datetime.datetime.now()
                        list_row.append(createdate)

                        all_results.append(list_row)

                    # Convert the collected data to a DataFrame
                        df_new = pd.DataFrame(all_results, columns=[
                            'Trendshift_Id', 'Repo_Name', 'Github', 'Website', 'Description',
                            'Language', 'Star', 'Fork', 'Create_Date'
                        ])
        return df_new
    
################ TRANSFORM FUNCTION ##################
df=Extract()

def ktothousand(value):
    if isinstance(value, str):
        try:
                    # Safely evaluate the expression replacing 'k' with '*1000'
            return eval(value.replace('k', '*1000'))
        except:
            return value
    return value

    # Task 2: Delete rows with 'NoInfo' and write notes as a report
def drop_no_info(df):
    no_info_indices = df[df['Repo_Name'] == 'NoInfo'].index
    df_cleaned = df.drop(no_info_indices)
    return df_cleaned


################ TRANSFORM FUNCTION ##################


def Transform():
      df_cleaned = drop_no_info(df)

    # Apply the conversion function to the 'Star' and 'Fork' columns
      df_cleaned['Star'] = df_cleaned['Star'].apply(ktothousand)
      df_cleaned['Fork'] = df_cleaned['Fork'].apply(ktothousand)


      return df_cleaned

def Load():
          # Define PostgreSQL connection parameters
    user = 'postgres'
    password = 'erdem1234'
    host = 'localhost'
    port = '5432'
    database = 'Trendshift_Upwork'

    # Create a connection engine to PostgreSQL using SQLAlchemy
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    # Define the name of the table where you want to store the DataFrame
    table_name = 'kimlervarmis'  # Replace with your actual table name

    # Upload the DataFrame to PostgreSQL
    Transform().to_sql(table_name, engine, if_exists='append', index=False)

    print(f"DataFrame successfully uploaded to table '{table_name}' in PostgreSQL.")


Load()

last_row=Transform().iloc[-1,0].tolist()





