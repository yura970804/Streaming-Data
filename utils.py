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

def sort_tid(df):
    times = []
    for i in range(len(df)):
        tid = df.iloc[i,]._id
        time = find_tweet_timestamp_post_snowflake(tid)
        times.append((df.index[i],time))
    times.sort(key=lambda x:x[1])
    
    sorted_data = []
    for a,b in times:
        tid = df.loc[a]._id
        sorted_data.append(tid)
    return sorted_data

def save_df(list_, file_name):
    df = pd.DataFrame(list_)    
    df.rename(columns = {'0' : '_id'}, inplace = True)    
    df.to_csv(file_name, mode='w', index=None)
    return df

