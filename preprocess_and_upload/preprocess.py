import os
from math import ceil, floor
from copy import deepcopy

import numpy as np
from rich.progress import track

#paths and headers of tables

#Do Foreign key check!

#TODO: Store for every table number of incorrect lines
#headers in dict tables_info may not be needed;
#we're not scanning a single mixed up table after all

#create a table and populate it

def create_tables():#(file_name,header):

    def day_of_week_generator(curr_day, prev_day, prev_day_week):
        def day_of_week_loop(starting_day, day_diff):
            days_of_week = {
                "Monday"    : 0,
                "Tuesday"   : 1,
                "Wednesday" : 2,
                "Thursday"  : 3,
                "Friday"    : 4,
                "Saturday"  : 5,
                "Sunday"    : 6
            }
            #take a starting day of DB  -  final day * mod7
            return (days_of_week[starting_day] + day_diff)%7

        #why this ? since you might have more days between one and the previous 
        month_days_prev = {
            1 : 31,
            2 : 28+floor(prev_day["year"]%4),
            3 : 31,
            4 : 30,
            5 : 31,
            6 : 30,
            7 : 31,
            8 : 31,
            9 : 30,
            10 : 31,
            11 : 30,
            12 : 31
        } 
        month_days_curr = deepcopy(month_days_prev)
        month_days_curr[2] = 28+floor(curr_day["year"]%4),
        if (curr_day["year"] == prev_day["year"]) & (curr_day["month"] == prev_day["month"]):
            day_diff = int(curr_day["day"]) - int(prev_day["day"])
        elif curr_day["year"] == prev_day["year"]:
            day_diff = curr_day["day"] + sum([i for i in range(month_days_curr[int(curr_day["month"])], month_days_prev[int(prev_day["month"])])]) - prev_day["day"]
        else:
            #to write
        return day_of_week_loop(prev_day_week, day_diff)

    vendor_dict = {}
    cpu_dict = {}
    geography_dict = {}
    time_dict = {}

    data_folder = "original_dataset"
    tables_info = {
        "vendor" :
            #".."vai alla tabella precedente
            {"path" : os.path.join("..", data_folder, "vendor.csv"),
            "header" : ["vendor_code", "name"]},
        "cpu" :
            {"path" : os.path.join("..", data_folder, "cpu.csv)",
            "header" : ["cpu_code", "brand", "series", "name", "n_cores", "socket"]},
        "geography" :
            {"path" : os.path.join("..", data_folder, "geography.csv"),
            "header":["geo_code","continent","country","region","currency"]},
        "time" :
            {"path" : os.path.join("..", data_folder, "time.csv"),
            "header" : ["time_code", "year", "month", "day", "week", "quarter", "day_of_week"]},
        "fact" :
            {"path" : os.path.joi"..", n(data_folder, "fact.csv"),
            "header" : ["Id", "cpu_code", "time_code", "geo_code", "vendor_code", "sales_usd", "sales_currency", "cost"]}
    }
    
    errors_count = {
        "vendor" :
            {"primary_key_error" : 0,
            "erroneous_field" : 0
            },
        "cpu" :
            {"primary_key_error" : 0,
            "erroneous_field" : 0
            },
        "geography" :
            {"primary_key_error" : 0,
            "erroneous_field" : 0
            },
        "time" :
            {"primary_key_error" : 0,
            "erroneous_field" : 0
            },
        "fact" :
            {"primary_key_error" : 0,
            "erroneous_field" : 0,
            "foreign_key_error" : 0
            }
    }

    prev_day = {
        "year"  : "2013",
        "month" : "3",
        "day"   : "21"
    }
    prev_day_week = "Thursday"

    #os library since adapts the paths in different OS
    #table_name = vendor, info = path and headers 
    for table_name, info in tables_info.items(): 
        with open(os.path.join("cleaned_dataset", f"{table_name}.csv"), mode="w+") as To_Populate:  #file to populate
            
            To_Populate.write(f"{','.join(info["header"])}\n") #####write the header on top
            
            #pick from the path...time.csv etc. only read
            with open(info["path"], 'r') as source:
                
                
                fact_counter = 1
                for row in track(csv.DictReader(source)):
                    line_check = True
                    new_row = ""
                    if table_name == "vendor": #open vendor.csv
                        for row_key, row_value in row.items():
                            if row_key == "vendor_code":
                                if vendor_dict.get(row_value):
                                    line_check = False
                                    errors_count["vendor"]["primary_key_error"] += 1
                                    break
                                else:
                                    #write primary key constraint enforcement
                                    #I LOVE IT!!!
                                    vendor_dict[row_value] = {}
                                    p_key = row_value #to retrieve primary key for next iterations along row

                                    #this will be the new rows but only in the CSV
                                    new_row = f"{new_row},{row_value}"
                            else: 
                                #here populate everything after the primary key
                                #this will come aft3rwards at the 
                                #end of each iteration
                                if row_value:
                                    vendor_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["vendor"]["erroneous_field"] += 1
                                    del vendor_dict[p_key]
                                    break


                    if table_name == "cpu":
                        for row_key, row_value in row.items():
                            if row_key == "cpu_code":
                                if cpu_dict.get(row_value):
                                    line_check = False
                                    errors_count["cpu"]["primary_key_error"] += 1
                                    break
                                else:
                                    #write primary key constraint enforcement
                                    #I LOVE IT!!!
                                    cpu_dict[row_value] = {}
                                    p_key = row_value #to retrieve primary key for next iterations along row

                                    #this will be the new rows but only in the CSV
                                    new_row = f"{new_row},{row_value}"
                            else: 
                                #this will come aft3rwards at the 
                                #end of each iteration

                                #if row value has none then do the else otherwise keep it
                                if row_value:
                                    cpu_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["cpu"]["erroreous_field"] += 1
                                    del cpu_dict[p_key]
                                    break


                    elif table_name=="geography":
                        #basically this below gives already a dicitonary
                        #vendor_code:1
                        #name:1StWaves
                        for row_key, row_value in row.items():
                            if row_key=="geo_code":
                                #check if value already in the dictionary, then break and exclude the row
                                if geography_dict.get(row_value):
                                    line_check = False
                                    errors_count["geography"]["primary_key_error"] += 1
                                    break #do not consider the line with same PK_ID
                                else:
                                    #geneate the vendor dict: {1:{}}
                                    geography_dict[row_value]={}
                                    #now create the primary key 
                                    p_key=row_value
                                    
                                    #print in the csv like this (for now empty = ,1 ... ,2 etc.)
                                    new_row=f"{new_row},{row_value}"
                                    
                            else:
                                #populate the dictionary with the pk and other values
                                if row_value:
                                    geography_dict[p_key][row_key]=row_value
                                    #print (geography_dict)
                                #{1:{'name':'1StWabe Technologies'}}
                                    #add this to the coming .csv file
                                    new_row=f"{new_row},{row_value}"
                                
                                #isn´t this too much ?
                                else:
                                    line_check = False
                                    errors_count["cpu"]["erroreous_field"] += 1
                                    del geography_dict[p_key]
                                    break

                    elif table_name == "time":
                        for row_key, row_value in row.items():
                            if row_key == "time_code":

                                #prendi colum time code, se il valore in 
                                #time_code dict e´ gia´ la, break 
                                if time_dict.get(row_value):
                                    line_check = False
                                    errors_count["time"]["primary_key_error"] += 1
                                    #keep track of incorrect lines somewhere
                                    break
                                else:
                                    
                                    #if key not in dictionary, populate since 
                                    #the column is the one of primary key
                                    time_dict[row_value] = {}
                                    p_key = row_value
                                    new_row = f"{new_row},{row_value}"
                            elif row_key == "year":
                                if row_value == p_key[:4]:
                                    time_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    time_dict[p_key][row_key] = p_key[:4]
                                    errors_count["time"]["erroreous_field"] += 1
                                    new_row = f"{new_row},{p_key[:4]}"

                            elif row_key == "month":
                                if row_value < 10:
                                    if row_value == p_key[5]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[5]
                                        errors_count["time"]["erroreous_field"] += 1
                                        new_row = f"{new_row},{p_key[5]}"
                                else:
                                    if row_value == p_key[4:6]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[4:6]
                                        errors_count["time"]["erroreous_field"] += 1
                                        new_row = f"{new_row},{p_key[4:6]}"
                            
                            elif row_key == "day":
                                if row_value < 10:
                                    if row_value == p_key[7]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[7]
                                        errors_count["time"]["erroreous_field"] += 1
                                        new_row = f"{new_row},{p_key[7]}"
                                else:
                                    if row_value == p_key[6:8]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[6:8]
                                        errors_count["time"]["erroreous_field"] += 1
                                        new_row = f"{new_row},{p_key[6:8]}"

                            else:
                                if int(row_value) <= 53:
                                    time_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    errors_count["time"]["erroreous_field"] += 1
                                    pass #to implement
                            
                            #adding quarters

                            #ceil is the intero maggiore ex 1,2 = 2

                            #create quarter on top since not present
                            quarter = ceil(4*int(time_dict[p_key]["month"])/12)
                            time_dict[p_key]["quarter"] = quarter
                            new_row = f"{new_row},{quarter}"

                            #adding days of week (hard)
                            #il prev day li stai aggiornando di continuo dentro il loop

                            day_of_week = day_of_week_generator(time_dict[p_key], prev_day, prev_day_week)
                            time_dict[p_key]["day_of_week"] = day_of_week

                            #for csv table
                            new_row = f"{new_row},{day_of_week}"    

                            #i+1 = i 
                            prev_day, prev_day_week = time_dict[p_key], day_of_week

                    #choose table
                    if table_name=="fact":
                        for row_key, row_value in row.items():
                            #use first header for PK unicity
                            if row_key=="Id":
                                #if in fact_dict there is already a pk which is the same, then skip it (will be double)
                                pass
                                 
                            # OR key
                            if (row_key == "gpu_code") | (row_key == "ram"):

                                #if column is gpu or ram,if there is a vlue break (not need to have it
                                
                                if row_value:
                                    line_check = False
                                    break
                                else:
                                    #else pass to the next column 
                                    pass
                                   
                                else:
                                    pass
                            elif row_key == "cpu_code":
                                #enforce foreign key constraint
                                #check if the cpu:_code of the fact exists in the dimension table
                                #se esiste, in the cpu_dict, foreign key enforcement is confirmed, then new row.......
                                if cpu_dict.get(row_value):

                                    #csv
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break
                            elif row_key == "time_code":
                                #enforce foreign key constraint
                                #check if the time:_code of the fact exists in the dimension table
                                #se esiste, in the cpu_dict, foreign key enforcement is confirmed, then new row.......
                                if time_dict.get(row_value):
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break
                            elif row_key == "geo_code":
                                #enforce foreign key constraint
                                #check if the geo:_code of the fact exists in the dimension table
                                #se esiste, in the cpu_dict, foreign key enforcement is confirmed, then new row.......
                                if geo_dict.get(row_value):
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break
                            elif row_key == "vendor_code":
                                #enforce foreign key constraint
                                #check if the geo:_code of the fact exists in the dimension table
                                #se esiste, in the cpu_dict, foreign key enforcement is confirmed, then new row.......
                                if vendor_dict.get(row_value):
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break

                            #check if there is a NON - Float and put only the first 2f 
                            else:
                                if row_value:
                                    try:
                                        row_value = float(row_value)
                                        new_row = f"{new_row},{row_value:.2f}"
                                    except ValueError:
                                        line_check = False
                                        errors_count["fact"]["erroreous_field"] += 1
                                        break
                                else:
                                    line_check = False
                                    errors_count["fact"]["erroreous_field"] += 1
                                    break
                                if row_key == "sales_uds":
                                    cost = row_value*np.random.norm(0.9, 0.05)
                                    
                        new_row = f"{new_row},{cost:.2f}"
                        #counter if everything is fine for the PK            
                        fact_counter += 1
                        new_row = f",{fact_counter}{new_row}"
                    #populate all the CSV  
                    if line_check:
                        To_Populate.write(f"{new_row[1:]}\n")

    #TODO: print dict of errors
    for table_name, errors in errors_count.items():
        print(f"{table_name} table; errors:")
        tot_errors = sum(errors.values())
        for name_error, error in errors.items():
            print(f"{name_error}: {error}")
        print(f"tot erros: {tot_errors}")
    

#main, to import the functions to other codes and projects 
if __name__ == "__main__":
    create_tables()





