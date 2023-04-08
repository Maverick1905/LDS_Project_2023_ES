from math import ceil, floor
from copy import deepcopy

import numpy as np

days_of_week = {
    "Monday"    : 0,
    "Tuesday"   : 1,
    "Wednesday" : 2,
    "Thursday"  : 3,
    "Friday"    : 4,
    "Saturday"  : 5,
    "Sunday"    : 6
}


def day_of_week_loop(starting_day, day_diff):
    days_of_week_reversed = {v : k for k, v in days_of_week.items()}
    #take a starting day of DB  -  final day * mod7
    final_day =  (days_of_week[starting_day] + day_diff)%7
    return days_of_week_reversed[final_day]


#prev_day seed: 20130321
def evaluate_day_diff(curr_day, prev_day):
    
    month_days_prev = {
        1 : 31,
        2 : 28+floor((int(prev_day["year"])%4+1)/4), #anno bisestile o no (if 28/4 + 1 else no bisestile)
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
    #deepcopied current and previous

    #prev / curr can have different dictionary of days of the month / year (bisestile o no)
    month_days_curr = deepcopy(month_days_prev)
    month_days_curr[2] = 28+floor((int(curr_day["year"])%4+1)/4),

    curr_day_d, prev_day_d = int(curr_day["day"]), int(prev_day["day"])
    curr_day_m, prev_day_m = int(curr_day["month"]), int(prev_day["month"])
    curr_day_y, prev_day_y = int(curr_day["year"]), int(prev_day["year"])
    
    #Define day_diff
    #if date in same year & month
    if (curr_day["year"] == prev_day["year"]) & (curr_day["month"] == prev_day["month"]):
        day_diff = curr_day_d - prev_day_d

    #same year but different month  
    elif curr_day["year"] == prev_day["year"]:

        #gli dai il giorni del mese corrent(finale) + girni mesi totali dal inizio fino a mese finale -1 (-mese finale)
        day_diff = curr_day_d + sum([month_days_curr[i] for i in range(prev_day_m, curr_day_m)]) - prev_day_d
    else:
        #different years, months etc.
        day_diff = sum([month_days_prev[i] for i in range(prev_day_m, 13)])  - prev_day_d \\
        #prev day year + 1 (the year after the start date+ curren day year - cureent. 
        #i%4 !=0 non bisestile
        #years in the middle
            + sum([365 if i%4 != 0 else 366 for i in range(prev_day_y+1, curr_day_y)]) \\
            #dal first of year to mese girno corrento + curr day month (1+28+31+30+12 for ex. ) 
            + sum([month_days_curr[i] for i in range(1, curr_day_m)]) + curr_day_d
    # use function based on inputs week day . day diff.
    return day_diff

def day_of_week_generator(curr_day, prev_day, prev_day_week):
    day_diff = evaluate_day_diff(curr_day, prev_day)
    return day_of_week_loop(prev_day_week, day_diff)