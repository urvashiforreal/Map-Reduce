# What is the average trip time for different pickup locations?
from mrjob.job import MRJob
from mrjob.step import MRStep
# We extend the MRJob class 
# This includes our definition of map and reduce functions
class Task4(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper, reducer=self.combiner),
          
            MRStep(
                reducer=self.reducer
            )
        ]
    # mapper function to find trip duration from each location
    def mapper(self, _, line):
        org = line.split(",")
        if (org[1] != "tpep_pickup_datetime"):
                
                drophrlist = org[2].split()
                drophr = drophrlist[1].split(':')[0]
                dropmin = drophrlist[1].split(':')[1]
                
                pickhrlist =   org[1].split()      
                pickhr = pickhrlist[1].split(':')[0]
                pickmin = pickhrlist[1].split(':')[1]
                dd = int(drophrlist[0].split('-')[0])
                dm = int(drophrlist[0].split('-')[1])
                dy = int(drophrlist[0].split('-')[2])
                pd = int(pickhrlist[0].split('-')[0])
                pm = int(pickhrlist[0].split('-')[1])
                py = int(pickhrlist[0].split('-')[2])

                y = dy - py
                m = dm  - pm 
                d = dd - pd
                mi = 0
                if (y == 0 and m == 0 and d == 0):
                        mi = (int(drophr)*60 + int(dropmin)) - (int(pickhr)*60 + int(pickmin))
                elif (y == 0 and m == 0 and d > 0):                        
                        mi = d*24*60 + (int(drophr)*60 + int(dropmin)) - (int(pickhr)*60 + int(pickmin))
                yield (org[7], (mi,1))
    # combiner function to find average trip time from each location            
    def _reducer_combiner(self, location, duration):
        avg = 0
        count = 0
        for loc, c in duration:
            avg = (avg * count + loc * c) / (count + c)
            count += c
        return (location, (avg, count))
    # combiner function to find average trip time from each location            
    def combiner(self, location, duration):
        location, (avg, count) = self._reducer_combiner(location, duration)
        #yield ("Pick up location", "Average Trip Time (mins)")
        yield (None, (location,str(round(avg))))
    # reducer to sort based on total revenue in descending order
    def reducer(self, _, word_counts):
        yield ("location","Trip Time (Mins)")        
        for count,key in sorted(word_counts):
            yield (count,key)  
    
    
if __name__ == '__main__':
    Task4.run()

""" Command:
python Task4.py input > out4.txt

"""
