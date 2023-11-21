#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
import sys

def process_comments(consumer, topic_names, condition_to_cut="EOF"):
    user_comments_cons1 = {}

    for msg in consumer:
        value = msg.value.decode('utf-8')
        #if value.strip().startswith("comment"):
        line_parts = value.split(' ')

        if len(line_parts) > 4:
           user2 = line_parts[2]
           comment_text_cons1 = ' '.join(line_parts[4:])

           if comment_text_cons1.startswith('"') and comment_text_cons1.endswith('"'):
              comment_text_cons1 = comment_text_cons1[1:-1]
              comment_text_cons1 = comment_text_cons1.replace('\\"', '"').replace('\\r', '')

              if user2 not in user_comments_cons1:
                 user_comments_cons1[user2] = []

              user_comments_cons1[user2].append(comment_text_cons1)

        if condition_to_cut in value:
           break
            
    sorted_user_comments_cons1 = {user: user_comments_cons1[user] for user in sorted(user_comments_cons1)}
    return sorted_user_comments_cons1

def main():
    topic_names = sys.argv[1]
 
    consumer_config = {
        'bootstrap_servers': 'localhost:9092'
    }

    consumer = KafkaConsumer(topic_names, **consumer_config)
    consumer.subscribe([topic_names])

    comments_data_cons1 = process_comments(consumer, topic_names)

    json_output = json.dumps(comments_data_cons1, indent=4, ensure_ascii=False)
    print(json_output)

    consumer.close()

if __name__ == '__main__':
    main()

