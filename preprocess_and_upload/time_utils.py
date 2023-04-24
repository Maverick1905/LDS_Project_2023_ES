from math import ceil, floor
from copy import deepcopy

import numpy as np

#numbers associated to days' names
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
    """
    Given a starting day of week and a difference in number of days,
    returns day of week of final day using modulo operand.

    params:
        - starting_day: name of starting day of week;
        - day_diff: difference in number of days.
    
    return:
        - final_day: day of week of final day.
    """
    days_of_week_reversed = {v : k for k, v in days_of_week.items()}
    final_day =  (days_of_week[starting_day] + day_diff)%7
    return days_of_week_reversed[final_day]


def evaluate_day_diff(curr_day, prev_day):
    """
    Evaluate difference in number of days given 2 dates expressed as a
    combination of day, month and year; takes into account all possible cases
    (days in same month and year, days in different months but same year,
    days in different years).

    params:
        - curr_day and prev_day: dictionaries of the form:

            curr_day (or prev_day) = {
                "day"   : ... ,
                "month" : ... ,
                "year"  : ... ,
                ...
            }
    
    return:
        - day_diff: difference in number of days
    """
    
    #number of days of given month for previous day's year
    month_days_prev = {
        1 : 31,
        2 : 28+floor(((int(curr_day["year"])-1)%4+1)/4),#account for leap years
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

    #number of days of given month for current day's year
    month_days_curr = deepcopy(month_days_prev)
    #account for leap years
    month_days_curr[2] = 28+floor(((int(curr_day["year"])-1)%4+1)/4)

    #variables to improve redability
    curr_day_d, prev_day_d = int(curr_day["day"]), int(prev_day["day"])
    curr_day_m, prev_day_m = int(curr_day["month"]), int(prev_day["month"])
    curr_day_y, prev_day_y = int(curr_day["year"]), int(prev_day["year"])
    
    #if date in same year & month:
    #simply take difference of 2 days of month
    if (curr_day["year"] == prev_day["year"]) & (curr_day["month"] == prev_day["month"]):
        day_diff = curr_day_d - prev_day_d

    #same year but different month:
    #take current day's day of month, add number of days of previous months
    #up until previous day's month, and then subtract previous day's day of month
    elif curr_day["year"] == prev_day["year"]:

        day_diff = curr_day_d + sum([month_days_curr[i] for i in range(prev_day_m, curr_day_m)]) - prev_day_d

    #most general case: take take current day's day of month,
    #add number of days from the beginning of its year up until it,
    #add number of days of previous years up until the year after
    #previous day's year, add number of days from previous day up until
    #the end of its year
    else:
        day_diff = sum([month_days_prev[i] for i in range(prev_day_m, 13)])  - prev_day_d \
        + sum([365 if i%4 != 0 else 366 for i in range(prev_day_y+1, curr_day_y)]) \
        + sum([month_days_curr[i] for i in range(1, curr_day_m)]) + curr_day_d

    return day_diff

def day_of_week_generator(curr_day, prev_day, prev_day_week):
    """
    Generates day of week of current day expressed as a date,
    given a previous day, expressed as a date as well, and its day of week.
    First evaluate difference in number of days and then exploit starting
    day of week using modulo operation.

    params:
        - curr_day and prev_day: dictionaries of the form:

            curr_day (or prev_day) = {
                "day"   : ... ,
                "month" : ... ,
                "year"  : ... ,
                ...
            }

        - prev_day_of_week: name of day of week as a string.
    
    return:
        - name of day of week of current day as a string.
    """
    day_diff = evaluate_day_diff(curr_day, prev_day)
    return day_of_week_loop(prev_day_week, day_diff)
