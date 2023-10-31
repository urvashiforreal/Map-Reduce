# What are the different payment types used by customers and their count? The final results should be in a sorted format.
from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class 
# This includes our definition of map and reduce functions
class Task3(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_extract_paymenttype, reducer=self.combine_payments),
            MRStep(
                reducer=self.reduce_sort_counts
            )
        ]

        
    # combine function to find sum of counts of each payment type
    def combine_payments(self, word, counts):
        yield (None, (word,sum(counts)))
        
    # reducer function to sort based on payment type
    def reduce_sort_counts(self, _, word_counts):
        yield ("Payment_type","Count")
        for word,counts in sorted(word_counts):
            yield (word,counts)
            
    # mapper function to get different payment types
    def mapper_extract_paymenttype(self, _, line):
        org = line.split(",")
        if (org[9] != "payment_type") :
            yield (org[9], 1)

    

    
if __name__ == '__main__':
    Task3.run()

""" Command:
python Task3.py input > out3.txt

"""
