import pandas as pd
import arrow

from ..describe import describe, Description
from ..io import readCsv

@describe(
    Description('hourly_messages', 'Aggregate message events by hour.', dockerImage='kitware/pysciencedock')
        .input('table', 'initial timestamped messages table', type='file', deserialize=readCsv)
        .output('grouped', 'The rolled up table', type='new-file', serialize=lambda df, fileName: df.to_csv(fileName))
)
def hourly_messages(table):
    return convertGeoAppMessagesToHourly(table)

def count(A):
    return A.count()

def group_data_by_hour(df):
    df.set_index('posted_date', inplace=True)
    df.sort_index(inplace=True)
    for col in df.columns: _ = df.pop(col)
    df['num_messages'] = 1
    df = df.resample('H').agg({'num_messages':count})
    return df

def convertGeoAppMessagesToHourly(df):
    # convert from nanosecs to secs
    df['posted_date'] = df['posted_date']/1000
    # convert to a list of dictionaries, so we can go through the data and convert the date columns,
    # there is undoubtedly a way to do this in pandas, but I can't figure it out 
    df_as_dicts = df.to_dict('records')
    # build a new list of dicts that has the datetimes converted so pandas can read them
    newdicts = []
    for row in df_as_dicts:
        newdicts.append(
                    {'posted_date':arrow.get(row['posted_date']).format()
                    })
    # make a new dataframe that has the  datetime string and some other columns
    new_df = pd.DataFrame(newdicts)
    # interpret as a datatime so pandas can understand the values
    new_df['posted_date'] =  pd.to_datetime(new_df['posted_date'])
    grouped = group_data_by_hour(new_df)
    # add columns
    grouped['date'] = grouped.index.date
    grouped['time'] = grouped.index.time
    grouped['hour'] = grouped.index.hour
    grouped['day']= grouped.index.day
    grouped['year'] = grouped.index.year
    return grouped


