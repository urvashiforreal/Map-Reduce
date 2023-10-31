# Which pickup location generates the most revenue? 
from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class MyMapReduce(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_location, reducer=self.combine_location_revenue),
          
            MRStep(
                reducer=self.reduce_sort_total_revenue
            )
        ]
    # mapper function to extract location and total revenue
    def mapper_extract_location(self, _, line):
        org = line.split(",")
        if (org[7] != "PULocationID") :
            yield (org[7], float(org[16]))
        
    # combiner function to find sum of revenue from location
    def combine_location_revenue(self, word, counts):
        yield (None, (sum(counts),word))

    
    # reducer to sort based on total revenue in descending order
    def reduce_sort_total_revenue(self, _, word_counts):
        yield ("Pick up location","Revenue")        
        for count,key in sorted(word_counts, reverse=True):
            yield (key,count)

if __name__ == '__main__':
    MyMapReduce.run()

""" Command:
python MyMapReduce.py input > out2.txt

"""
