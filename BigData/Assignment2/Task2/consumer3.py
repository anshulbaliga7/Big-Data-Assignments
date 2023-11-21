#!/usr/bin/env python3

from kafka import KafkaConsumer
import sys
import json

def process_messages(consumer, topic_names, condition_to_cut="EOF"):
    user_activity_cons3 = {}

    while True:
        msg = next(consumer)
        value = msg.value.decode('utf-8')

        if condition_to_cut in value:
            break

        line_parts = value.split()

        if len(line_parts) >= 4:
            action_type = line_parts[0]
            user = line_parts[1]
            target_user = line_parts[2]
            post_id = line_parts[3]
            shared_users = [user.strip() for user in line_parts[4:] if user.strip()]

            if target_user not in user_activity_cons3:
                user_activity_cons3[target_user] = {'likes': 0, 'comments': 0, 'shares': 0}

            if action_type == 'like':
                user_activity_cons3[target_user]['likes'] += 1
            elif action_type == 'comment':
                user_activity_cons3[target_user]['comments'] += 1
            elif action_type == 'share':
                user_activity_cons3[target_user]['shares'] += len(shared_users)

    sorted_user_activity = {user: user_activity_cons3[user] for user in sorted(user_activity_cons3)}
    return sorted_user_activity

def popularity_calculator(user_activity_cons3):
    user_popularity_cons3 = {}

    for user, activities in user_activity_cons3.items():
        popularity = (activities['likes'] + 20 * activities['shares'] + 5 * activities['comments']) / 1000
        user_popularity_cons3[user] = popularity

    return user_popularity_cons3


def main():
    topic_names = sys.argv[1:]

    consumer_config = {
        'bootstrap_servers': 'localhost:9092'
    }

    topic_name = topic_names[2]

    consumer = KafkaConsumer(topic_name, **consumer_config)
    consumer.subscribe([topic_name])

    messages_data_cons3 = process_messages(consumer, topic_names)

    popularity_data_cons3 = popularity_calculator(messages_data_cons3)
    json_output = json.dumps(popularity_data_cons3, indent=4)

    print(json_output)
    consumer.close()

if __name__ == '__main__':
    main()

