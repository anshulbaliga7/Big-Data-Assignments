#!/usr/bin/env python3

import sys
import json

current_player = None
total_runs = 0
total_balls = 0

for line in sys.stdin:
    line = line.strip()
    data = line.split('\t')  

    if len(data) != 2:
        continue

    name = data[0]
    strike_rate = float(data[1])

    if current_player is None:
        current_player = name

    if name == current_player:
        total_runs += strike_rate
        total_balls += 1
    else:
        if total_balls > 0:
            average_strike_rate = total_runs / total_balls
            average_strike_rate = round(average_strike_rate, 3)
            result = {"name": current_player, "strike_rate": average_strike_rate}
            print(json.dumps(result))

        current_player = name
        total_runs = strike_rate
        total_balls = 1

if current_player is not None and total_balls > 0:
    average_strike_rate = total_runs / total_balls
    average_strike_rate = round(average_strike_rate, 3)
    result = {"name": current_player, "strike_rate": average_strike_rate}
    print(json.dumps(result))

