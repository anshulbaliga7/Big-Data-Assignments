#!/usr/bin/env python3

import sys

for line in sys.stdin:
    fields = line.strip().split('\t')
    record_type = fields[0]

    if record_type == "order":
        order_id, customer_id, product_id, quantity, price = fields[1:]
        print(f"{product_id}\torder\t{customer_id}\t{quantity}\t{price}")
    elif record_type == "review":
        review_id, product_id, customer_id, rating, review_text = fields[1:]
        print(f"{product_id}\treview\t{customer_id}\t{rating}\t{review_text}")

