#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    if len(fields) >= 7:
        rating = int(fields[5])
        if int(fields[1]) == int(fields[4]) and rating < 3:
            print(f"{fields[0]}\treview\t{fields[2]}\t{fields[3]}\t{fields[4]}\t{rating}")

