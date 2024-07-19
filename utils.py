import json


# function to deliver callback
def delivery_report(err, msg): 
    if err is not None:
        print(f'Message delivery failed: {err}')
    else: 
        print(f'Message delivered to {msg.topic()}') 



def serialize_json(data): 
    return json.dumps(data).encode('utf-8')