import os
import pandas as pd
import numpy as np
import pymysql
from dotenv import load_dotenv

def process_retention_episode():

    load_dotenv()
    # Establish a connection to the MySQL database
    connection = pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT'))
    )
    # Fetch tracker data from MySQL database
    cursor = connection.cursor()
    query = "select drifter_id from drifters"
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    tdf = pd.DataFrame(data, columns=column_names)
    sdf = pd.DataFrame({
        'Tracker ID': [],
        'Tracker Name': [],
        'Batch ID': [],
        'Total active time (days)': [],
        'Total retention time (days)': [],
        'Total transport time (days)': [],
        'Number of retention episodes': [],
        'Average duration of retention episodes (days)': [],
        'Average duration of retention episodes (hours)': [],
        'Median duration of retention episodes (hours)': [],
        'Maximum duration of retention episodes (hours)': [],
        'Minimum duration retention episode (hours)': []
    })
    for i in range(len(tdf)):
        print(f"Processing tracker ID: {tdf['id'][i]}")
        input_value = str(tdf['id'][i])
        # Fetch data from MySQL database
        cursor = connection.cursor()
        query = "select * from drifters_history where drifter_id = " + input_value
        cursor.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        # Create a DataFrame from the fetched data
        df = pd.DataFrame(data, columns=column_names)

        # iterate through each row and select
        acc_time = 0
        df['1hrMovingAvg'] = 0
        for ind in df.index:
            vel_sum = 0
            counter = 0
            new_ind = ind
            while (acc_time <= 3600):
                acc_time += df['second_diff'][new_ind]
                vel_sum += df['velocity'][new_ind]
                counter = counter + 1
                new_ind = new_ind + 1
                df['1hrMovingAvg'][ind] = vel_sum/counter
            acc_time = 0

        df['retention_code'] = np.where(df['1hrMovingAvg'] < 0.1, 1, 0)
        df['retention_time_days'] = None
        df['retention_time_hours'] = None

        retain = 0
        retain_time = 0
        retain_count = 0
        for ind in df.index:
            if df['retention_code'][ind] == 1:
                if (ind == 0):
                    retain_time = df['second_diff'][ind]
                else:
                    retain_time = retain_time + df['second_diff'][ind-1]
                retain = 1

            if df['retention_code'][ind] == 0 and retain == 1:
                df['retention_time_days'][ind-1] = retain_time/86400
                df['retention_time_hours'][ind-1] = retain_time/3600
                retain_count = retain_count + 1
                retain_time = 0
                retain = 0

            if ind == df.index[-1]:
                if df['retention_code'][ind] == 1 and retain == 1:
                    df['retention_time_days'][ind] = retain_time/86400
                    df['retention_time_hours'][ind] = retain_time/3600
                    retain_count = retain_count + 1
                    retain_time = 0
                    retain = 0



        # Summarize the retention points

        if pd.isnull(df['second_diff'].iloc[-1]):
            df['second_diff'].iloc[-1] = 0

        data_to_concat = pd.DataFrame({
            'Tracker ID': [input_value],
            'Tracker Name': [tdf['name'][i]],
            'Batch ID': [tdf['country'][i]],
            'Total active time (days)': [(df['second_diff'].sum()-df['second_diff'].iloc[-1])/86400],
            'Total retention time (days)': [df['retention_time_days'].sum()],
            'Total transport time (days)': [0],
            'Number of retention episodes': [retain_count],
            'Average duration of retention episodes (days)': [df['retention_time_days'].mean()],
            'Average duration of retention episodes (hours)': [df['retention_time_hours'].mean()],
            'Median duration of retention episodes (hours)': [df['retention_time_hours'].median()],
            'Maximum duration of retention episodes (hours)': [df['retention_time_hours'].max()],
            'Minimum duration retention episode (hours)': [df['retention_time_hours'].min()]
        })
        sdf = pd.concat([sdf, data_to_concat], ignore_index=True)

        # Save the processed data to a CSV file
        # Check if the folder exists, otherwise create it
        folder_path = 'drifter_data_output/'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        df.to_csv('drifter_data_output/'+input_value+'.csv', index=False)

    sdf['Total transport time (days)'] = sdf['Total active time (days)'] - sdf['Total retention time (days)']
    sdf.to_csv('drifter_data_output/retention_summary.csv', index=False)
    connection.close()
    print("Data processing complete.")