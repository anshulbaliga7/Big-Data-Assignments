#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    if len(fields) == 6:
        product_id, record_type, quantity, *rest = fields
        if quantity.isdigit() and rest[-1].isdigit():
            quantity = int(quantity)
            rating = int(rest[-1])
            print(f"{product_id}\t{rating}\t{quantity}")

