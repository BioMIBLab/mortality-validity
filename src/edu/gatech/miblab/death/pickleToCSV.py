#
# pickleToCSV.py
#	  Ryan Hoffman, 2017
#
# Usage: python pickleToCSV.py infilename [outfilename]
#
# Convert Hang's pickled OrderedDicts of
# requent patterns to CSV files
#

import sys
import pickle
import csv
from collections import *

with open(sys.argv[1],"r") as infile:
    fp = pickle.load(infile)

fp2 = OrderedDict(sorted(fp.items(), key=lambda r: r[1]))

with open(sys.argv[2] if len(sys.argv)>2 else "output.csv","w") as outfile:
    w = csv.writer(outfile)
    for pattern in fp.items():
        a = [pattern[1]]
        for r in pattern[0]:
            a.append(r)
        if len(a)>2: w.writerow(a)