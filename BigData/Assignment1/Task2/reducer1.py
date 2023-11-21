#!/usr/bin/env python3

import sys

current_product = None
product_data = {}

for line in sys.stdin:
    fields = line.strip().split('\t')
    product_id, record_type, *rest = fields

    if current_product is None:
        current_product = product_id

    if product_id != current_product:
        if "order" in product_data and "review" in product_data:
            for order in product_data["order"]:
                for review in product_data["review"]:
                    order_customer_id = order.split('\t')[0]
                    review_customer_id = review.split('\t')[0]
                    if order_customer_id == review_customer_id:
                        print(f"{current_product}\t{order}\t{review}")

        product_data = {}
        current_product = product_id

    if record_type == "order":
        if "order" not in product_data:
            product_data["order"] = []
        product_data["order"].append("\t".join(rest))
    elif record_type == "review":
        if "review" not in product_data:
            product_data["review"] = []
        product_data["review"].append("\t".join(rest))

if "order" in product_data and "review" in product_data:
    for order in product_data["order"]:
        for review in product_data["review"]:
            order_customer_id = order.split('\t')[0]
            review_customer_id = review.split('\t')[0]
            if order_customer_id == review_customer_id:
                print(f"{current_product}\t{order}\t{review}")

