import os
import csv
import pyodbc
from rich.progress import track

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
    cn, cursor = open_conn()

    for table_name, path in tables_path.items():
        with open (path,'r') as table_file:
            reader=csv(DictReader(table_file))
            commit=0
            for row in track(reader):
                #line to populate to db
                to_send = ""
                for row_value in row.values():
                    to_send = f"{to_send},{row_value}"   
                try:

                    #table name is the name of our tables (in dict above)
                    query = f"INSERT INTO ['pileggi_mura_DB'].[{table_name}] VALUES ({to_send[1:]});" #SQL
                    cursor.execute(query)
                except Exception as e:
                    print(f"Failed upload upon query:\n{query}")
                    #if except stoppati 
                    close_conn(cn, cursor)
            # commit once every 100 rows/queries
                commit += 1
                if commit == 100:
                    cn.commit()
                    commit = 0
        # if there are "leftover" rows, commit them too.
            if commit > 0:
                cn.commit()
        print(f"Upload of table {table_name} performed succesfully.")
    # close connection
    close_conn(cn, cursor)

if __name__ == "__main__":
    load_tables()
