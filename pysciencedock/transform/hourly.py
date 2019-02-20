import pandas as pd

from ..describe import describe, Description
from ..io import readCsv

@describe(
    Description('hourly', 'Aggregates events by hour.', dockerImage='kitware/pysciencedock')
        .input('table1', 'The first data table', type='file', deserialize=readCsv)
        .output('grouped', 'The rolled up table', type='new-file', serialize=lambda df, fileName: df.to_csv(fileName))
)

def count(A):
    return A.count()

def group_data_by_hour(df):
    df.set_index('pickup_datetime', inplace=True)
    df.sort_index(inplace=True)
    for col in df.columns: _ = df.pop(col)
    df['num_pickups'] = 1
    df = df.resample('H').agg({'num_pickups':count})
    return df

def convertGeoAppToHourly(df):
    # convert from nanosecs to secs
    df['dropoff_datetime'] = df['dropoff_datetime']/1000
    df['pickup_datetime'] = df['pickup_datetime']/1000
    # convert to a list of dictionaries, so we can go through the data and convert the date columns,
    # there is undoubtedly a way to do this in pandas, but I can't figure it out 
    df_as_dicts = df.to_dict('records')
    # build a new list of dicts that has the datetimes converted so pandas can read them
    newdicts = []
    for row in dicts:
        newdicts.append(
                    {'pickup_datetime':arrow.get(row['pickup_datetime']).format(),
                     'pickup_latitude': row['pickup_latitude'],
                     'dropoff_longitude': row['dropoff_longitude']
                    })
    # make a new dataframe that has the  datetime string and some other columns
    new_df = pd.DataFrame(newdicts)
    # interpret as a datatime so pandas can understand the values
    new_df['pickup_datetime'] =  pd.to_datetime(new_df['pickup_datetime'])
    grouped = group_data_by_hour(new_df)
    # add columns
    grouped['hour'] = grouped.index.hour
    grouped['day']= grouped.index.day
    grouped['year'] = grouped.index.year
    return grouped

def hourly(table1):
	return convertGeoAppToHourly(table)
