#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    line = line.strip()

    if line.startswith("[") or line.endswith("]"):
        continue
    if line.endswith(","):
        line = line[:-1]
    
    data = json.loads(line)
    
    name = data.get("name")
    runs = int(data.get("runs"))
    balls = int(data.get("balls"))

    if balls == 0:
       strike_rate = 0.0
    else:
       strike_rate = (runs / balls) * 100

    print(f'{name}\t{strike_rate:.3f}')
