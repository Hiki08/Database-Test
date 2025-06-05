import mysql.connector as mariadb
import numpy as np

mariadb_connection = None
create_cursor = None

def SqlConnection():
    global create_cursor, mariadb_connection
    
    mariadb_connection = mariadb.connect(user='hpi.python', password='hpi.python', database='fc_1_data_db', host='192.168.2.148', port=3306)
    # mariadb_connection = mariadb.connect(user='hpi.python', password='hpi.python', host='192.168.2.148', port=3306)

    create_cursor = mariadb_connection.cursor()

def CreateDatabase(databaseName):
    global create_cursor

    create_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {databaseName}")

def ShowDatabase():
    global create_cursor

    create_cursor.execute("SHOW DATABASES")

    for x in create_cursor:
        print(x)

def CreateTable1():
    global create_cursor

    create_cursor.execute("CREATE TABLE table_test1 (Name VARCHAR(64), Date VARCHAR(64), MODELCODE VARCHAR(64))")

def CreateTable2():
    global create_cursor

    create_cursor.execute("CREATE TABLE PROCESS1_DATA (Name VARCHAR(64), Date VARCHAR(64), MODELCODE VARCHAR(64))")

def CreateProcess1Table():
    global create_cursor

    create_cursor.execute("""
        CREATE TABLE IF NOT EXISTS process1_data(
            Process_1_DATA_No VARCHAR(64),
            Process_1_DateTime VARCHAR(64),
            Process_1_Model_Code VARCHAR(64),
            Process_1_S_N VARCHAR(64),
            Process_1_ID VARCHAR(64),
            Process_1_NAME VARCHAR(64),
            Process_1_Regular_Contractual VARCHAR(64),
            Process_1_Em2p VARCHAR(64),
            Process_1_Em2p_Lot_No VARCHAR(64),
            Process_1_Em3p VARCHAR(64),
            Process_1_Em3p_Lot_No VARCHAR(64),
            Process_1_Harness VARCHAR(64),
            Process_1_Harness_Lot_No VARCHAR(64),
            Process_1_Frame VARCHAR(64),
            Process_1_Frame_Lot_No VARCHAR(64),
            Process_1_Bushing VARCHAR(64),
            Process_1_Bushing_Lot_No VARCHAR(64),
            Process_1_ST VARCHAR(64),
            Process_1_Actual_Time VARCHAR(64),
            Process_1_NG_Cause VARCHAR(64),
            Process_1_Repaired_Action VARCHAR(64)
        )
        """)

def ShowTables():
    global create_cursor

    create_cursor.execute("SHOW TABLES")

    for x in create_cursor:
        print(x)

def DeleteTable(tableName):
    global create_cursor
    
    create_cursor.execute(f"DROP TABLE IF EXISTS {tableName}")

def InsertDataToTable(tableName):
    global create_cursor, mariadb_connection
    
    sqlStatement = f"INSERT INTO {tableName} (Name, Date, MODELCODE) VALUES ('CARL', '29/05/2025', '213P')"
    create_cursor.execute(sqlStatement)
    mariadb_connection.commit()

def InsertDataToTable2(tableName):
    global create_cursor, mariadb_connection

    sqlStatement = f"INSERT INTO {tableName} (Name, Date, MODELCODE) VALUES (%s, %s, %s)"
    itemsToInsert = ['CARL', '29/05/2025', '213P']

    create_cursor.execute(sqlStatement, itemsToInsert)
    mariadb_connection.commit()

def InsertDataToProcess1Table(df):
    global create_cursor, mariadb_connection

    # Replace NaN values with None
    df = df.replace({np.nan: None})
    
    # First, get all existing datetimes from the database
    create_cursor.execute("SELECT Process_1_DateTime FROM process1_data")
    existing_datetimes = [row[0] for row in create_cursor.fetchall()]
    
    # Filter out records that already exist in the database
    new_records = []
    for record in df.values.tolist():
        if record[1] not in existing_datetimes:  # Process_1_DateTime is at index 1
            new_records.append(record)
    
    if not new_records:
        print("No new records to insert")
        return
    
    # Create the SQL insert statement
    insert_query = """
        INSERT INTO process1_data (
            Process_1_DATA_No,
            Process_1_DateTime,
            Process_1_Model_Code,
            Process_1_S_N,
            Process_1_ID,
            Process_1_NAME,
            Process_1_Regular_Contractual,
            Process_1_Em2p,
            Process_1_Em2p_Lot_No,
            Process_1_Em3p,
            Process_1_Em3p_Lot_No,
            Process_1_Harness,
            Process_1_Harness_Lot_No,
            Process_1_Frame,
            Process_1_Frame_Lot_No,
            Process_1_Bushing,
            Process_1_Bushing_Lot_No,
            Process_1_ST,
            Process_1_Actual_Time,
            Process_1_NG_Cause,
            Process_1_Repaired_Action
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Execute the insert only for new records
    create_cursor.executemany(insert_query, new_records)
    mariadb_connection.commit()
    print(f"Successfully inserted {len(new_records)} new records into process1_data table")

def SelectAllDataFromTable(tableName):
    global create_cursor

    sqlStatement = f"SELECT * FROM {tableName}"
    create_cursor.execute(sqlStatement)
    myresult = create_cursor.fetchall()
    print(myresult)

def SelectSpecificDataFromTable(tableName, columnName):
    global create_cursor

    sqlStatement = f"SELECT * FROM {tableName} WHERE {columnName} = 'CARL'"
    create_cursor.execute(sqlStatement)
    myresult = create_cursor.fetchall()
    print(myresult)

def CloseConnection():
    global mariadb_connection

    mariadb_connection.close()
