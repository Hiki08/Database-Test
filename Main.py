#%%
from Imports import *
import mysql.connector as mariadb

import CsvReader
import Sql

Sql.SqlConnection()

# Sql.CreateProcess1Table()

CsvReader.ReadProcess1Csv()
Sql.InsertDataToProcess1Table(CsvReader.dfVt1)

# Sql.InsertDataToTable2('table_test1')
# Sql.SelectSpecificDataFromTable('table_test1', 'Name')

# %%
