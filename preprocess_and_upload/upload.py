import os
import csv
import pyodbc
import itertools
from rich.progress import track
from rich.console import Console

def open_conn():
    server = "lds.di.unipi.it"  # lds.di.unipi.it
    database = "pileggi_mura_DB"
    username = "pileggi_mura"
    password = "E53B9FVM" 
    connectionString = (
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )

    cn = pyodbc.connect(connectionString)
    return cn, cn.cursor()

def close_conn(cn, cursor):#remember to close it always
    cursor.close()
    cn.close()

def load_tables():
    data_folder = "cleaned_dataset"
    tables_path = {
        "vendor" : os.path.join("..", data_folder, "vendor.csv"),
        "cpu" : os.path.join("..", data_folder, "cpu.csv"),
        "geography" : os.path.join("..", data_folder, "geography.csv"),            
        "time" : os.path.join("..", data_folder, "time.csv"),
        "fact" : os.path.join("..", data_folder, "fact.csv")
    }
    tables_header = {
        "vendor" : {
            "vendor_code" : "int",
            "name" : "str"},
        "cpu" : {
            "cpu_code" : "int",
            "brand" : "str",
            "series" : "str",
            "name" : "str",
            "n_cores" : "int",
            "socket" : "str"},
        "geography" : {
            "geo_code" : "int",
            "continent" : "str",
            "country" : "str",
            "region" : "str",
            "currency" : "str"},
        "time" : {
            "time_code" : "int",
            "year" : "int",
            "month" : "int",
            "day" : "int",
            "week" : "int",
            "quarter" : "str",
            "day_of_week" : "str"},
        "fact" : {
            "Id" : "int",
            "cpu_code" : "int",
            "time_code" : "int",
            "geo_code" : "int",
            "vendor_code" : "int",
            "sales_usd" : "float",
            "sales_currency" : "float",
            "cost" : "float"}
        }

    cn, cursor = open_conn()

    for table_name, path in tables_path.items():
        print(f"Uploading table [{table_name}]...")
        with open(path, 'r') as table_file:
            reader = csv.DictReader(table_file)
            commit = 0
            for row in track(reader):
                #line to populate to db
                to_send = ""
                for row_key, row_value in row.items():
                    if tables_header[table_name][row_key] == "str":
                        #check if there's an apostrophe in the string and "escape" it
                        position = row_value.find("'")
                        if position != -1:
                            row_value = f'{row_value[:position]}{row_value[position+1:]}'
                        to_send = f"{to_send},'{row_value}'"
                    else:
                        to_send = f"{to_send},{row_value}"
                    #to_send = f"{to_send},{row_value}"   
                try:

                    #table name is the name of our tables (in dict above)
                    query = f"INSERT INTO [pileggi_mura].[{table_name}] VALUES ({to_send[1:]});" #SQL
                    cursor.execute(query)
                # commit once every 100 rows/queries
                    commit += 1
                    if commit == 100:
                        cn.commit()
                        commit = 0
                except Exception as e:
                    print(e)
                    print(f"Failed upload upon query:\n{query}")
                    #if except stoppati 
                    close_conn(cn, cursor)
        # if there are "leftover" rows, commit them too.
            if commit > 0:
                cn.commit()
        print(f"Upload of table {table_name} performed succesfully.")
    # close connection
    close_conn(cn, cursor)

if __name__ == "__main__":
    console = Console()
    console.log("Loading data onto server")
    load_tables()
    console.log("Upload completed")