# How does revenue vary over time? Calculate the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep

# function to return day of the week from day, month and year
def day_of_week(year, month, day):
    t = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4]
    if (month < 3):
        year = year-1
    return (year + int(year/4) - int(year/100) + int(year/400) + t[month-1] + day) % 7

# function to return code as follows
# 1 if weekday and day time
# 2 if weekday and night time
# 3 if weekend and day time
# 4 if weekend and night time
def getMonthAndCode(ss):
    #mydate = datetime.strptime(ss, '%d-%m-%Y %H:%M')
    monthno = int(ss.split()[0].split('-')[1])
    month = ''
    monthlist = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    #month = mydate.strftime("%B")
    month = monthlist[monthno-1]
     
    hour = int(ss.split()[1].split(':')[0])
    dd = int(ss.split()[0].split('-')[0])
    mm = int(ss.split()[0].split('-')[1])
    yy = int(ss.split()[0].split('-')[2])
    code = 0
    weekday = day_of_week(dd,mm,yy)
    day = [6,7,8,9,10,11,12,13,14,15,16,17,18]
    night = [19,20,21,22,23,0,1,2,3,4,5]
    if hour in day and weekday in range(0,5):
        code = 1
    elif hour in night and weekday in range(0,5):
        code = 2
    elif hour in day and weekday in range(5,7):
        code = 3
    else:
        code = 4
    return((month,code))
# We extend the MRJob class 
# This includes our definition of map and reduce functions
class Task6(MRJob):
    # mapper function to return code and revenue based on pick up date
    def mapper(self, _, line):
        org = line.split(",")
        if (org[1] != "tpep_pickup_datetime"):
                month,code = getMonthAndCode(org[1])
                yield (month, (code,org[16]))
                
    # combiner function to combine revenue
    def _reducer_combiner(self, month, coderevenue):
        code1sum = 0
        code2sum=0
        code3sum=0
        code4sum = 0
        for code, revenue in coderevenue:
            if code == 1 :
                code1sum = code1sum + float(revenue)
            elif code == 2:
                code2sum = code2sum + float(revenue)
            elif code == 3:
                code3sum = code3sum + float(revenue)
            elif code == 4:
                code4sum = code4sum + float(revenue)
        return (month, (code1sum, code2sum,code3sum,code4sum ))

    # reducer function to reduce  based on month  
    def reducer(self, month, coderevenue):
        month, (code1sum, code2sum,code3sum,code4sum ) = self._reducer_combiner(month, coderevenue)
        yield (month, ("Week Day, Day Time ",code1sum))
        yield (month, ("Week Day, Night Time ",code2sum))
        yield (month, ("Week End, Day Time ",code3sum))
        yield (month, ("Week End, Night Time ",code4sum))
     
             
    
if __name__ == '__main__':
    Task6.run()

""" Command:
python Task6.py input > out6.txt

"""
