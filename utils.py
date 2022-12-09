from datetime import datetime

def find_tweet_timestamp_post_snowflake(tid):
    offset = 1288834974657
    tstamp = (tid >> 22) + offset
    time = datetime.fromtimestamp(tstamp/1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S.%f")

def find_machine_id(tid):
    binaryNum = format(tid, 'b')
    binaryNum = '000'+binaryNum
    machine_id = int(binaryNum[42:52], 2)
    return machine_id
