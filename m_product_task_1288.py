import glob
import pandas as pd
from datetime import datetime,timedelta
import glob
import numpy as np
pd.set_option('display.max_columns', None)

list_call_log = glob.glob(r'D:\rerun_5\Product project\final_call_log_*.pq')

# transform attempt
attempt = pd.read_csv(r'D:\rerun_5\Product project\upper_query.csv')
attempt.head(2)

#clean attempt
attempt['callee'] = attempt['callee'].str.replace(' ','').str[-9:]
attempt['attempt_date'] = attempt['attempt_datetime'].str[:10].astype('datetime64[ns]')
attempt['attempt_datetime'] = attempt['attempt_datetime'].str[:19].astype('datetime64[ns]')
attempt['caller'] = attempt['caller'].str.replace(' ','').str[-9:]

# basket order
attempt.sort_values(['attempt_datetime'],ascending= True).groupby('attempt_date').head(5)
count_order = attempt.groupby(['attempt_date','caller','route_id','callee'],as_index=False).agg({'order_id':'count'})
count_order.rename(columns={'order_id':'is_basket_order'})
basket_order = pd.merge(count_order[count_order.order_id != 1],attempt,how='inner',on=['attempt_date','caller','route_id','callee'])
basket_order.rename(columns = {'order_id_y':'order_id'},inplace=True)

# min attempt_datetime basket order
min_attempt_basket_order = basket_order.groupby(['order_id'],as_index=False).agg({'attempt_datetime':'min'})
first_route_basket =  basket_order.merge(min_attempt_basket_order,how='inner',on=['order_id','attempt_datetime'])
first_route_basket.rename(columns={'route_id':'basket_1st_route_id'},inplace=True)
attempt = attempt.merge(first_route_basket[['order_id','basket_1st_route_id']],how='left',on=['order_id'])
attempt.basket_1st_route_id = attempt.basket_1st_route_id.astype('str')
attempt.order_id = attempt.order_id.astype('str')

# transform order_id of all order in basket order
attempt.order_id = attempt.apply(lambda x: x.basket_1st_route_id if x.basket_1st_route_id != 'nan' else x.order_id, axis=1)

# Filter max attempt_datetime by order_id
max_attempt = attempt.groupby(['attempt_date','caller','callee'], as_index=False).agg({'attempt_datetime':'max'})
attempt = pd.merge(attempt,max_attempt, on=['attempt_date','caller','callee','attempt_datetime'], how = 'inner')
attempt = attempt.drop_duplicates().reset_index(drop=True)

# Create columns last_attempt_datetime to section call log by different driver deli to the same callee on the same day
attempt.sort_values(['attempt_datetime'],ascending= True,inplace=True)
attempt['last_attempt_datetime'] = attempt.groupby(['attempt_date','callee'])['attempt_datetime'].shift(1)

# LM end time of order
lm_end = attempt.groupby('order_id').agg({'attempt_date':'max'})

# Main flown to calculate index (tử số)
first_index = 0
second_index = 0
third_index = 0
fourth_index = 0
a = 0
loop = 0

for i in list_call_log:
    print(loop)
    # Clean call log
    raw_call_log = pd.read_parquet(i)
    raw_call_log = raw_call_log[['caller','callee','started_at','direction']]
    ## clean call log datetime
    raw_call_log['attempt_date'] = raw_call_log['started_at'].str[:10].astype('datetime64[ns]')
    raw_call_log['started_at'] = raw_call_log['started_at'].str[:19].astype('datetime64[ns]')
    ## Binory incoming call columns
    raw_call_log['is_incoming_call'] =[1 if i.direction == 3 else 0 for i in raw_call_log.itertuples()]
    raw_call_log.callee = raw_call_log.apply(lambda x: x.caller if x.is_incoming_call != 0 else x.callee, axis = 1)
    raw_call_log = raw_call_log[['attempt_date','callee','started_at','is_incoming_call']]

    raw_df = attempt.merge(raw_call_log, on=['attempt_date','callee'], how='inner')

    # loop > 1 recheck unpass order
    if a != 0:
        raw_df = pd.concat([raw_df,unpass_order])

    lm_status = raw_df.groupby(['order_id'],as_index= False).agg({'attempt_date':'max'})
    qualifield_order = lm_end.merge(lm_status, on=['order_id','attempt_date'],how='inner')
    unpass_order = raw_df[~raw_df.order_id.isin(qualifield_order.order_id.unique())]
    pass_order = raw_df[raw_df.order_id.isin(qualifield_order.order_id.unique())]
    pass_order = pass_order[pass_order.attempt_datetime >= pass_order.started_at] 
    pass_order = pd.concat([pass_order[pass_order.last_attempt_datetime.isna() == True],pass_order[(pass_order.last_attempt_datetime.isna() == False)&(pass_order.started_at >= pass_order.last_attempt_datetime)]])
    pass_order = pass_order[['order_id','started_at','attempt_date','is_incoming_call']]

    # first index : Số lượng incoming call from khách hàng chưa bao giờ nhận được cuộc gọi từ rider
    thresh_hold = pass_order.groupby(['order_id'],as_index = False).agg({'is_incoming_call':'min'})
    thresh_hold.rename(columns = {'is_incoming_call':'min_incoming_call'}, inplace = True)
    no_rider_call = pass_order[pass_order.order_id.isin(thresh_hold[thresh_hold.min_incoming_call != 0].order_id.unique())]
    first_index += no_rider_call.is_incoming_call.sum()
    
    # df to calculate second ,third ,fourth index : Số lượng incoming call from khách hàng có calling timestamp < 3 ngày kể từ 1st call attempt của tài xế
    ## create df that contain only first_rider_call_att and first_callee_call_att 
    order_have_1st_att = pass_order[~pass_order.order_id.isin(no_rider_call.order_id.unique())]

    ### df first_call_att
    first_call_att = order_have_1st_att[order_have_1st_att.is_incoming_call != 1].groupby('order_id',as_index = False).agg({'started_at':'min'})
    first_call_att.rename(columns = {'started_at':'first_driver_call'}, inplace = True)
    order_have_1st_att = order_have_1st_att.merge(first_call_att, on= 'order_id', how = 'left')
    order_have_1st_att = order_have_1st_att[order_have_1st_att.started_at >= order_have_1st_att.first_driver_call]

    ### df first_incoming_call_att
    first_incoming_call_att = order_have_1st_att[order_have_1st_att.is_incoming_call !=0].groupby('order_id',as_index = False).agg({'started_at':'min'})
    first_incoming_call_att.rename(columns = {'started_at':'first_callee_call'},inplace = True)
  
    ### result
    result = first_call_att.merge(first_incoming_call_att, on = 'order_id',how = 'left')

    # Calculate fourth index
    fourth_index += len(result[result.first_callee_call.isna()== True])
    
    # Calculate second and third index
    ## Second index
    ### Raw df and clean raw df to calculate second & third index
    result = result[result.first_callee_call.isna() == False]
    len(result)
    result['first_driver_call_date'] = result.first_driver_call.dt.strftime('%d-%m-%Y')
    result.first_driver_call_date = pd.to_datetime(result.first_driver_call_date)
    result['first_callee_call_date'] = result.first_callee_call.dt.strftime('%d-%m-%Y')
    result.first_callee_call_date = pd.to_datetime(result.first_callee_call_date)
    result = result.assign(calling_timestamp = lambda x: result.first_callee_call_date - result.first_driver_call_date)
    
    ### Calculate second index
    timestamp_below_3 = result[result.calling_timestamp < pd.Timedelta(3,unit= 'd')]
    second_index += timestamp_below_3.calling_timestamp.count()

    ### Calculate third index
    timestamp_above_3 = result[~result.order_id.isin(timestamp_below_3.order_id.unique())] 
    third_index += timestamp_above_3.calling_timestamp.count()
    a += 1

# Main flown to calculate fifth index (mẫu số)

fifth_index = 0
## Calculate fifth index
for i in list_call_log:
    # Clean call log
    raw_call_log = pd.read_parquet(i)
    raw_call_log = raw_call_log[['caller','callee','started_at','direction']]

    ## clean call log datetime
    raw_call_log['attempt_date'] = raw_call_log['started_at'].str[:10].astype('datetime64[ns]')
    raw_call_log['started_at'] = raw_call_log['started_at'].str[:19].astype('datetime64[ns]')

    ## Binory incoming call columns
    raw_call_log['is_incoming_call'] =[1 if i.direction == 3 else 0 for i in raw_call_log.itertuples()]
    raw_call_log.callee = raw_call_log.apply(lambda x: x.caller if x.is_incoming_call != 0 else x.callee, axis = 1)
    raw_call_log = raw_call_log[['attempt_date','callee','started_at','is_incoming_call']]

    fifth_index += raw_call_log[raw_call_log.is_incoming_call == 1].is_incoming_call.count()

print(first_index)
print(second_index)
print(third_index)
print(fourth_index)
print(fifth_index)




    
    


