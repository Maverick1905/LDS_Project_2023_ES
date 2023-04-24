import os
import csv
from math import ceil, floor
from copy import deepcopy

import numpy as np
from rich.progress import track

from time_utils import days_of_week, day_of_week_loop, evaluate_day_diff, day_of_week_generator

def create_tables():

    np.random.seed(1)

    #dictionaries for primary foreign key constraint enforcement
    vendor_dict = {}
    cpu_dict = {}
    geo_dict = {}
    time_dict = {}

    data_folder = "original_dataset"
    tables_info = {
        "vendor" :
            {"path" : os.path.join("..",data_folder, "vendor.csv"),
            "header" : ["vendor_code", "name"]},
        "cpu" :
            {"path" : os.path.join("..",data_folder, "cpu.csv"),
            "header" : ["cpu_code", "brand", "series", "name", "n_cores", "socket"]},
        "geography" :
            {"path" : os.path.join("..",data_folder, "geography.csv"),
            "header":["geo_code","continent","country","region","currency"]},
        "time" :
            {"path" : os.path.join("..",data_folder, "time.csv"),
            "header" : ["time_code", "year", "month", "day", "week", "quarter", "day_of_week"]},
        "fact" :
            {"path" : os.path.join("..",data_folder, "fact.csv"),
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
            {"erroneous_field" : 0,
            "foreign_key_error" : 0
            }
    }

    #base date to evaluate day_of_week
    prev_day = {
        "year"  : "2013",
        "month" : "3",
        "day"   : "21"
    }
    prev_day_week = "Thursday"

    #dictionary to store sampled cost percentage w.r.t. sales_usd
    derived_cost_by_day= {}

    out_folder = "cleaned_dataset"
    if not os.path.exists(out_folder):
        os.makedirs(out_folder)
   
    for table_name, info in tables_info.items(): 
        with open(os.path.join("..",out_folder, f"{table_name}.csv"), mode="w+") as To_Populate:
        
            #header line
            To_Populate.write(f"{','.join(info['header'])}\n")
            
            with open(info["path"], 'r') as source:
                
                #counter for fact table's primary key
                fact_counter = 1

                for row in track(csv.DictReader(source)):
                    #control variable for erroneous lines; if True print line in file, do nothing otherwise
                    line_check = True
                    new_row = ""
                    if table_name == "vendor":
                        for row_key, row_value in row.items():
                            if row_key == "vendor_code":
                                #enforce primary key constraint
                                if vendor_dict.get(row_value):
                                    line_check = False
                                    errors_count["vendor"]["primary_key_error"] += 1
                                    break
                                else:
                                    vendor_dict[row_value] = {}
                                    #to retrieve primary key along the line
                                    p_key = row_value
                                    new_row = f"{new_row},{row_value}"
                            else:
                                #check for missing fields
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
                                    cpu_dict[row_value] = {}
                                    p_key = row_value
                                    new_row = f"{new_row},{row_value}"
                            else:
                                if row_value:
                                    cpu_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["cpu"]["erroneous_field"] += 1
                                    del cpu_dict[p_key]
                                    break


                    elif table_name == "geography":
                        for row_key, row_value in row.items():
                            if row_key == "geo_code":
                                if geo_dict.get(row_value):
                                    line_check = False
                                    errors_count["geography"]["primary_key_error"] += 1
                                    break
                                else:
                                    geo_dict[row_value] = {}
                                    p_key = row_value
                                    new_row=f"{new_row},{row_value}"
                                    
                            else:
                                if row_value:
                                    geo_dict[p_key][row_key]=row_value
                                    new_row=f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["cpu"]["erroneous_field"] += 1
                                    del geo_dict[p_key]
                                    break

                    elif table_name == "time":
                        for row_key, row_value in row.items():
                            if row_key == "time_code": 
                                if time_dict.get(row_value):
                                    line_check = False
                                    errors_count["time"]["primary_key_error"] += 1
                                    break
                                else:
                                    time_dict[row_value] = {}
                                    p_key = row_value
                                    new_row = f"{new_row},{row_value}"
                                
                            elif row_key == "year":
                                #check if year consistent with time_id, otherwise use time_id to retrie it
                                if row_value == p_key[:4]:
                                    time_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    time_dict[p_key][row_key] = p_key[:4]
                                    errors_count["time"]["erroneous_field"] += 1
                                    new_row = f"{new_row},{p_key[:4]}"
                                
                            elif row_key == "month":
                                #check if month consistent
                                if int(row_value) < 10:
                                    if row_value == p_key[5]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[5]
                                        errors_count["time"]["erroneous_field"] += 1
                                        new_row = f"{new_row},{p_key[5]}"
                                else:
                                    if row_value == p_key[4:6]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[4:6]
                                        errors_count["time"]["erroneous_field"] += 1
                                        new_row = f"{new_row},{p_key[4:6]}"
                                
                            elif row_key == "day":
                                #check if day consistent
                                if int(row_value) < 10:
                                    if row_value == p_key[7]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[7]
                                        errors_count["time"]["erroneous_field"] += 1
                                        new_row = f"{new_row},{p_key[7]}"
                                else:
                                    if row_value == p_key[6:8]:
                                        time_dict[p_key][row_key] = row_value
                                        new_row = f"{new_row},{row_value}"
                                    else:
                                        time_dict[p_key][row_key] = p_key[6:8]
                                        errors_count["time"]["erroneous_field"] += 1
                                        new_row = f"{new_row},{p_key[6:8]}"

                            else:
                                #generate day_of_week using day_of_week and date of previous line
                                day_of_week = day_of_week_generator(time_dict[p_key], prev_day, prev_day_week)
                                #find number of days from fist day of the year to the Sunday of the current day's week;
                                #we need this to verify week number of current line
                                days_to_sunday = 6 - days_of_week[day_of_week]
                                first_day_of_year = {
                                    "day" : 1,
                                    "month" : 1,
                                    "year" : time_dict[p_key]["year"]
                                    }
                                n_days_in_year = evaluate_day_diff(time_dict[p_key], first_day_of_year)
                                n_weeks = ceil((n_days_in_year + days_to_sunday)/7)

                                #check week number
                                if int(row_value) == n_weeks:
                                    time_dict[p_key][row_key] = row_value
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    errors_count["time"]["erroneous_field"] += 1
                                    time_dict[p_key][row_key] = n_weeks
                                    new_row = f"{new_row},{n_weeks}"
                            
                        if line_check:
                                
                            #find quarters using months
                            quarter = ceil(4*int(time_dict[p_key]["month"])/12)
                            time_dict[p_key]["quarter"] = f"Q{quarter}"
                            new_row = f"{new_row},Q{quarter}"
        
                            time_dict[p_key]["day_of_week"] = day_of_week
        
                            new_row = f"{new_row},{day_of_week}"    
        
                            #to use for week's day of next line
                            prev_day, prev_day_week = time_dict[p_key], day_of_week

                    if table_name=="fact":
                        for row_key, row_value in row.items():
                            #dummy primary key created by csv.DictReader
                            if row_key=="":
                                pass
                    
                            #dummy spurious id
                            elif row_key=="Id":
                                pass
                                 
                            #gpu_code and ram_code: discard lines where they are not null
                            elif (row_key == "gpu_code") | (row_key == "ram_code"):
                                if row_value:
                                    line_check = False
                                    break
                                else:
                                    pass

                            elif row_key == "cpu_code":
                                #enforce foreign key constraint for cpu_code through dictionary
                                if cpu_dict.get(f"{row_value[:-2]}"):
                                    new_row = f"{new_row},{row_value[:-2]}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1 
                                    break
 
                            elif row_key == "time_code":
                                #enforce foreign key constraint for time_code through dictionary
                                if time_dict.get(f"{row_value}"):
                                    new_row = f"{new_row},{row_value}"
                                    
                                    #add cost derived
                                    sales_day = row_value
                                    #if haven't sampled yet, then sample
                                    if not derived_cost_by_day.get(row_value):
                                        derived_cost_by_day[row_value] = np.random.normal(0.9,0.05)
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break
                            elif row_key == "geo_code":
                                #enforce foreign key constraint for geo_code through dictionary
                                
                                if geo_dict.get(f"{row_value}"):
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break
                            elif row_key == "vendor_code":
                                #enforce foreign key constraint for vendor_code through dictionary
                               
                                if vendor_dict.get(f"{row_value}"):
                                    new_row = f"{new_row},{row_value}"
                                else:
                                    line_check = False
                                    errors_count["fact"]["foreign_key_error"] += 1
                                    break

                            #give to sales_usd and sales_currency only the first 2f 
                            else:
                                if row_value:
                                    try:
                                        row_value = float(row_value)
                                        new_row = f"{new_row},{row_value:.2f}"
                                    except ValueError:
                                        line_check = False
                                        errors_count["fact"]["erroneous_field"] += 1
                                        break
                                else:
                                    line_check = False
                                    errors_count["fact"]["erroneous_field"] += 1
                                    break
                                #evaluate cost from sampled percentage * sales_usd
                                if row_key == "sales_uds":
                                    cost = row_value*derived_cost_by_day[sales_day]
                                    
                        #insert cost
                        if line_check:    
                            new_row = f"{new_row},{cost:.2f}"
                            new_row = f",{fact_counter}{new_row}"
                            fact_counter += 1
                            
                    #populate csv if everything went correctly
                    if line_check:
                        To_Populate.write(f"{new_row[1:]}\n")
    
    #print errors' summary
    for table_name, errors in errors_count.items():
        print(f"{table_name} table; errors:")
        tot_errors = sum(errors.values())
        for name_error, error in errors.items():
            print(f"{name_error}: {error}")
        print(f"tot erros: {tot_errors}")
    
if __name__ == "__main__":
    create_tables()
