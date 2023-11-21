#!/usr/bin/env python3

import sys

current_product = None
max_negative_rating = float('inf')
total_quantity = 0

for line in sys.stdin:
    product_id, rating, quantity = line.strip().split('\t')
    quantity = int(quantity)
    rating = int(rating)

    if current_product is None or product_id != current_product:
        if current_product is not None:
            if max_negative_rating != float('inf'):
                print(f"{current_product}\t{total_quantity}")
        
        current_product = product_id
        max_negative_rating = rating
        total_quantity = 0

    if rating < max_negative_rating:
        max_negative_rating = rating

    total_quantity += quantity

if current_product is not None:
    if max_negative_rating != float('inf'):
        print(f"{current_product}\t{total_quantity}")

