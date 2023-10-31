#!/usr/bin/env python

"""
Insert data into HBase with a Python script.
"""

import csv
import happybase
import time
import pandas as pd

batch_size = 1000
host = "ec2-54-164-149-197.compute-1.amazonaws.com"
file_path = ["/home/hadoop/yellow_tripdata_2017-03.csv","/home/hadoop/yellow_tripdata_2017-04.csv"]
row_count = 0
start_time = time.time()
table_name = "taxi"
rowc = 1
def insert_first_column(file):
    global rowc
    df = pd.read_csv(file )
    df.insert(0, 'New_ID', range(rowc, rowc + len(df)))
    df.to_csv(file)
    rowc = rowc+len(df)

def connect_to_hbase():
    """ Connect to HBase server.
    This will use the host, namespace, table name, and batch size as defined in
    the global variables above.
    """
    conn = happybase.Connection(host = host)
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch


def insert_row(batch, row):
    """ Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema:
        [ id, keyword, subcategory, type, township, city, zip, council_district,
          opened, closed, status, origin, location ]
    """
    batch.put(row[0], { "cf:VendorID": row[1], "cf:tpep_pickup_datetime": row[2], "cf:tpep_dropoff_datetime ": row[3],
                       "cf:passenger_count": row[4], "cf:trip_distance": row[5], "cf:RatecodeID ": row[6],
                       "cf:store_and_fwd_flag": row[7], "cf:PULocationID ": row[8], "cf:DOLocationID ": row[9],
                       "cf:payment_type": row[10], "cf:fare_amount": row[11], "cf:extra": row[12],"cf:mta_tax":row[13],"cf:tip_amount":row[14],"cf:tolls_amount":row[15],"cf:improvement_surcharge":row[16],"cf:total_amount":row[17],"cf:congestion_surcharge":row[18],"cf:airport_fee":row[19] })


def read_csv(file):
    csvfile = open(file, "r")
    csvreader = csv.reader(csvfile)
    return csvreader, csvfile

for f in file_path:
    insert_first_column(f)

# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
print ("Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size))
for f in file_path:
    csvreader, csvfile = read_csv(f)
print ("Connected to file. name: %s" % (file_path))

try:
    # Loop through the rows. The first row contains column headers, so skip that
    # row. Insert all remaining rows into the database.
    for row in csvreader:
        row_count += 1
        if row_count == 1:
            pass
        else:
            insert_row(batch, row)

    # If there are any leftover rows in the batch, send them now.
    batch.send()
finally:
    # No matter what happens, close the file handle.
    csvfile.close()
    conn.close()

duration = time.time() - start_time
print ("Done. row count: %i, duration: %.3f s" % (row_count, duration))
