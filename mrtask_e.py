# Calculate the average tips to revenue ratio of the drivers for different locations in sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep
# We extend the MRJob class 
# This includes our definition of map and reduce functions
class Task5(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_ratio, reducer=self.combine_ratio),
            MRStep(
                reducer=self.reduce_sort_locations
            )
        ]
    # mapper function to find tips to revenue ratio for each trip
    def mapper_extract_ratio(self, _, line):
        org = line.split(",")
        if (org[7] != "PULocationID"):
                tip_amount = float(org[13])
                total_amount = float(org[16])
                ratio = 0
                if (total_amount != 0):
                    ratio = tip_amount/total_amount
                yield (org[7], (ratio,1))
      
    # combiner function to find average of tips to revenue ratio for each location  
    def _reducer_combiner(self, location, ratio):
        avg, count = 0, 0
        for loc, c in ratio:
            avg = (avg * count + loc * c) / (count + c)
            count += c
        return (location, (avg, count))
      
    # combiner function to find average oftips to revenue ratio for each location        
    def combine_ratio(self, location, ratio):
        location, (avg, count) = self._reducer_combiner(location, ratio)
        yield (None, (location,round(avg,2)))
    # reducer function to sort data based on location            
    def reduce_sort_locations(self,_, location_counts):
        yield ("location Id","Average tips to revenue ratio")
        for key,count in sorted(location_counts):
            yield (key,count)
             
    
if __name__ == '__main__':
    Task5.run()

""" Command:
python Task5.py input > out5.txt

"""
