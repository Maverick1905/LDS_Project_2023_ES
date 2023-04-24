import os
import csv
import pyodbc
from rich.progress import track
from rich.console import Console

def write_log(tables_done, curr_row):
    """
    Write log file in case of a failure. Log file will have
    the following template:

    ### LOG FILE ###
    Completed tables:
    ###,###,###
    Number of committed rows:
    ###

    params:
        - tables_done: list containing names of the successfully
          uploaded tables;
        - curr_row: last row committed in currently uploading file.
    """
    tables = ""
    for table in tables_done:
        tables = f"{tables}{table},"
    with open("log.txt", 'w') as log:
        log.write(f"### LOG FILE ###\nCompleted tables:\n{tables[:-1]}\n")
        log.write(f"Number of committed rows:\n{curr_row}")

def read_log():
    """
    Searches for an existing log file, and takes the checkpoint information
    (tables_done and curr_row). If no log file is found, returns empty list
    and 0 as current row.
    """
    try:
        with open("log.txt", 'r') as log:
            rows = log.readlines()
            tables_done = rows[2].strip().split(',')
            curr_row = int(rows[4])
    except FileNotFoundError:
        tables_done, curr_row = [], 0
    return tables_done, curr_row

def delete_log():
    """
    Deletes log file once the whole upload is completed.
    """
    try:
        os.remove("log.txt")
    except FileNotFoundError:
        return

def open_conn():
    """
    Defines connection string and returns connection and cursor objects.
    """
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

def close_conn(cn, cursor):
    """
    Close connection and cursor objects.
    """
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

    #to account for different field types
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

    #reads log file if existing
    tables_done, curr_row = read_log()
    for table_name, path in tables_path.items():
        #if table already present in done tables, do nothing while iterating
        if table_name not in tables_done:
            print(f"Uploading table [{table_name}]...")
            with open(path, 'r') as table_file:
                reader = csv.DictReader(table_file)
                #counter variable for committing
                commit = 0
                #counter variable for log file
                lines_done = 0
                for row in track(reader):
                    #skip already committed lines
                    if lines_done >= curr_row:
                        to_send = ""
                        for row_key, row_value in row.items():
                            #fix string format
                            if tables_header[table_name][row_key] == "str":
                                #find apostrophes in string and remove them
                                position = row_value.find("'")
                                if position != -1:
                                    row_value = f'{row_value[:position]}{row_value[position+1:]}'
                                to_send = f"{to_send},'{row_value}'"
                            else:
                                to_send = f"{to_send},{row_value}"
                        try:

                            query = f"INSERT INTO [pileggi_mura].[{table_name}] VALUES ({to_send[1:]});"
                            cursor.execute(query)
                        except Exception as e:
                            #"23000" is primary key error: rather than stopping whole upload,
                            #simply skip row; otherwise break connection and write log
                            if e.args[0] != "23000":
                                print(e)
                                print(f"Failed upload upon query:\n{query}")
                                close_conn(cn, cursor)
                                write_log(tables_done, lines_done)
                                return
                        commit += 1
                        #commit every 100 lines and reset counter
                        if commit == 100:
                            cn.commit()
                            lines_done += commit
                            commit = 0
                    else:
                        lines_done += 1
                #commit remaining rows
                if commit > 0:
                    cn.commit()
            print(f"Upload of table {table_name} performed succesfully.")
            #record completed table for log file
            tables_done.append(table_name)
            #reset counter
            lines_done = 0
    #close connection
    close_conn(cn, cursor)
    delete_log()

if __name__ == "__main__":
    console = Console()
    console.log("Loading data onto server")
    load_tables()
    console.log("Upload completed")
