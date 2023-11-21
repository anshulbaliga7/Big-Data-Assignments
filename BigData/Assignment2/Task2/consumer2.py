#!/usr/bin/env python3

from kafka import KafkaConsumer
import sys
import json

def likes_calculator(consumer, topic_name, condition_to_cut="EOF"):
    user_likes_cons2 = {}

    for msg in consumer:
        value = msg.value.decode('utf-8')
        
        #if value.strip().startswith("like"):
        line_parts = value.split(' ')

        if len(line_parts) == 4:
           user_liking_post = line_parts[2]
           post_id = line_parts[3].strip()

           user_liking_post = user_liking_post.strip()

           if user_liking_post not in user_likes_cons2:
              user_likes_cons2[user_liking_post] = {}

           if post_id not in user_likes_cons2[user_liking_post]:
              user_likes_cons2[user_liking_post][post_id] = 0

           user_likes_cons2[user_liking_post][post_id] += 1
        if condition_to_cut in value:
           break 

    sorted_user_likes_cons2 = {user_liking_post: user_likes_cons2[user_liking_post] for user_liking_post in sorted(user_likes_cons2)}
    return sorted_user_likes_cons2

def main():
    topic_names = sys.argv[1:]

    consumer_config = {
        'bootstrap_servers': 'localhost:9092'
    }

    topic_name = topic_names[1]

    consumer = KafkaConsumer(topic_name, **consumer_config)
    consumer.subscribe([topic_name])

    likes_data_cons2 = likes_calculator(consumer, topic_name)
    
    json_output = json.dumps(likes_data_cons2, indent=4)
    print(json_output)
    
    consumer.close()

if __name__ == '__main__':
    main()

