#
# pickleToCSV.py
#	  Ryan Hoffman, 2017
#
# Convert Hang's pickled OrderedDicts of
# requent patterns to CSV files
#

import pickle
import csv
from collections import *

with open("mort2012_o_seqs_50.p","r") as infile:
    fp = pickle.load(infile)

fp2 = OrderedDict(sorted(fp.items(), key=lambda r: r[1]))

with open("csvfile.csv","w") as outfile:
    w = csv.writer(outfile)
    for pattern in fp.items():
        a = [pattern[1]]
        for r in pattern[0]:
            a.append(r)
        if len(a)>2: w.writerow(a)