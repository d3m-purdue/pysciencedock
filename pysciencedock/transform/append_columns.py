import pandas as pd

from ..describe import describe, Description
from ..io import readCsv

@describe(
    Description('Append columns', 'the columns of two data tables using a matching key.', dockerImage='kitware/pysciencedock')
        .input('table1', 'The first data table', type='file', deserialize=readCsv)
        .input('table2', 'The second data table', type='file', deserialize=readCsv)
        .output('combined', 'The combined table', type='new-file', serialize=lambda df, fileName: df.to_csv(fileName))
)
def append_columns(table1, table2):
   first_join_column = ['hour','day','year','date','time']
   second_join_column = first_join_column 
   result_df = pd.merge(table1, table2, how='outer', left_on=first_join_column, right_on=second_join_column)
   return result_df 
