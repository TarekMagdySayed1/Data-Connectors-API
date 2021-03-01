import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
import ast
import pandas as pd
import pandasql as ps
from datetime import datetime
from datetime import timedelta
import os


Gen_Token = os.environ['Gen_Token'] 
Gen_Pass = os.environ['Gen_Pass'] 
def convert(x):
    try:
        dt = ast.literal_eval(x)
        return pd.to_datetime(str(dt[0]) + '-' + str(dt[1]) + '-' + str(dt[2]) +  ' ' + str(dt[3]) + ':' + str(dt[4]) + ':' + str(dt[5]))
    except:
        return x.replace('None', '2015-01-01')


def get_interval_string_2(follow):
        """Function takes no paramters and returns an time interval in json format"""
        if follow == 'Yesterday':
            yesterday = datetime.now() - timedelta(1)
       
        return yesterday.strftime('%Y-%m-%d') + 'T03:00:01' + '/' + yesterday.strftime('%Y-%m-%d') + 'T23:59:59'

def shift_start(shift):
        
        if shift == '1':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T08:00:00' 
        if shift == '2':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T11:00:00' 
        if shift == '3':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T13:00:00' 
        return s

def shift_end(shift):
        if shift == '1':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T17:00:00' 
        if shift == '2':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T20:00:00' 
        if shift == '3':
            yesterday = datetime.now() - timedelta(1)
            s = yesterday.strftime('%Y-%m-%d') + 'T22:00:00' 
        return s

def nearest_start(dat):
    s =  min([pd.to_datetime(shift_start('1')),pd.to_datetime(shift_start('2')),pd.to_datetime(shift_start('3'))], key=lambda x: abs(x -pd.to_datetime( dat)))
    return s

def nearest_end(dat):
    hours = 9
    hours_added = timedelta(hours = hours)
    s =  min([pd.to_datetime(shift_start('1')) , pd.to_datetime(shift_start('2')),pd.to_datetime(shift_start('3'))], key=lambda x: abs(x -pd.to_datetime( dat)))
    return s + hours_added
    
    
def reformating(sec):
    if sec is not None:
        if sec >= 3600:
            hour = int(abs(round(int(sec / (60*60)),9)))
            minute = int(abs(round((sec - (hour*60*60))/60,9)))
            second = int(abs(round(sec - ((hour*60 + minute) * 60),9)))
        elif  sec < 3600 and sec >= 60:
            hour = 0
            minute = int(abs(round(int(sec / 60),9)))
            second = int(abs(round(sec - (minute * 60),9)))
        else:
            hour = 0
            minute = 0
            second = int(abs(round(sec, 9)))
    else:
        return sec
    return str(round(hour))+':'+str(round(minute))+':'+str(round(second))

def reformating_2(sec):
    if sec is not None:
        if sec >= 3600:
            hour = int(abs(round(int(sec / (60*60)),9)))
            minute = int(abs(round((sec - (hour*60*60))/60,9)))
            second = int(abs(round(sec - ((hour*60 + minute) * 60),9)))
        elif  sec < 3600 and sec >= 60:
            hour = 0
            minute = int(abs(round(int(sec / 60),9)))
            second = int(abs(round(sec - (minute * 60),9)))
        else:
            hour = 0
            minute = 0
            second = int(abs(round(sec, 9)))
    else:
        return sec
    return str(round(hour))+'h '+str(round(minute))+'m '+str(round(second)) + 's'

def get_back(sec):
    if sec is not None:
        sec_list = str(sec).split(':')
        total = int(sec_list[0]) * 3600 + int(sec_list[1]) * 60 + int(sec_list[2])
        return total
    else:
        return sec
    
def converted(x):
    if x is not None and isinstance(x,str) == True:
        return pd.to_timedelta(x).total_seconds()
    
def split_date(dt):
    return dt[10:]

def KPIs(AgentID):
    region = PureCloudPlatformClientV2.PureCloudRegionHosts.eu_west_1
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()
    apiClient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(Gen_Token, Gen_Pass)
    api_instance = PureCloudPlatformClientV2.AnalyticsApi(apiClient)
    body = PureCloudPlatformClientV2.UserDetailsQuery()
    x = get_interval_string_2('Yesterday')
    body = {
      "interval": x,
      "userFilters": [
          {
            "type": "or",
            "predicates": [
                {
                    "dimension": "userId",
                    "value": AgentID
                }
            ]
          }
          
      ],
      
      "paging": {
    "pageSize": 100,
    "pageNumber": 1
  }
    }
    
    
    try:
        api_response = api_instance.post_analytics_users_details_query(body)
        
        
       
        lst =[]
        lst_2 =[]
        lst_3 = []
        try:
            for row in api_response.user_details[0].primary_presence:
                #print(row)
                lst.append(ast.literal_eval(((str(row)).replace(", tzinfo=tzutc()", "").replace("datetime.datetime","'")).replace(")",")'").replace('\n','').replace("None", "'None'")))
            Abanoub = pd.DataFrame(lst)
            Abanoub['start_time'] = Abanoub['start_time'].apply(lambda x:convert(x))
            Abanoub['end_time'] = Abanoub['end_time'].apply(lambda x:convert(x))
            Abanoub['start_time'] = pd.to_datetime(Abanoub['start_time'])
            Abanoub['end_time'] = pd.to_datetime(Abanoub['end_time'])
            Abanoub['start_time'] = Abanoub['start_time'].fillna(pd.to_datetime('2015-01-01'))
            Abanoub['end_time'] = Abanoub['end_time'].fillna(pd.to_datetime('2015-01-01'))
            Abanoub['start_time'] = Abanoub['start_time'].dt.tz_localize('Etc/GMT')
            Abanoub['end_time'] = Abanoub['end_time'].dt.tz_localize('Etc/GMT')
            Abanoub['end_time'] = Abanoub['end_time'].dt.tz_convert('Etc/GMT')
            Abanoub['start_time'] = Abanoub['start_time'].dt.tz_convert('Etc/GMT-2')
            Abanoub['end_time'] = Abanoub['end_time'].dt.tz_convert('Etc/GMT-2')
            print(len(Abanoub))
            
            
    
            query = PureCloudPlatformClientV2.ConversationAggregationQuery()
            query.interval = get_interval_string_2('Yesterday')
            query.group_by = ['queueId','userId']
            query.metrics = ['tTalkComplete','tHeldComplete']
            query.filter = PureCloudPlatformClientV2.ConversationAggregateQueryFilter()
            query.filter.type = 'and'
            query.filter.clauses = [PureCloudPlatformClientV2.ConversationAggregateQueryClause()]
            query.filter.clauses[0].type = 'or'
            query.filter.clauses[0].predicates = [PureCloudPlatformClientV2.ConversationAggregateQueryPredicate()]
            query.filter.clauses[0].predicates[0].dimension = 'userId'
            query.filter.clauses[0].predicates[0].value = AgentID

        #tAnswered (count) / tTalkComplete (sum)
            analytics_api = PureCloudPlatformClientV2.AnalyticsApi(apiClient)
            query_result = analytics_api.post_analytics_conversations_aggregates_query(query)
            print(query_result)
        except ApiException as e:
            print("Exception when calling AnalyticsApi->post_analytics_users_details_query: %s\n" % e)
        login = ps.sqldf(""" select  min(end_time)  as LOGIN  
                                from Abanoub 
                                where  start_time  != "2015-01-01 02:00:00.000000"
                                and   end_time  != "2015-01-01 02:00:00.000000"
                                """)
        #logout = ps.sqldf(""" select start_time as logout
        #from Abanoub
        #where  start_time = (select max(start_time) from Abanoub where system_presence like "%OFFLINE%" and start_time != "2015-01-01 02:00:00.000000")
        #""")
        logout = ps.sqldf(""" select case when max(start_time) > max(end_time) then max(start_time) else max(end_time) end as LOGOUT  from Abanoub 
        """)
        OFF = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as OFFLINE
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        and system_presence like "%OFFLINE%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        BUSY = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as BUSY
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%BUSY%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        MEETING = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as MEETING
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%MEETING%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
       
        TRAINING = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as TRAINING
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%TRAINING%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        AWAY = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as AWAY
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%AWAY%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        AVAILABLE = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as AVAILABLE
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%AVAILABLE%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        BREAK = ps.sqldf(""" 
        select sum((julianday(end_time) - julianday(start_time)) * 86400.0 ) as BREAK
        from(
        select start_time, end_time, (julianday(end_time) - julianday(start_time)) * 86400.0 
        from Abanoub
        -- where  end_time != (select min(end_time) from Abanoub where system_presence like "%OFFLINE%" and end_time != '2015-01-01 02:00:00.000000')
        where system_presence like "%BREAK%"
        and end_time != '2015-01-01 02:00:00.000000') t
        """)
        ALL_Data = ps.sqldf(""" 
        select * , case when BREAK < 3600 then null else BREAK - 3600  end as EXCEEDED_BREAK
        from login
        join logout
        on 1=1
        join BUSY
        on 1=1
        join OFF
        on 1=1
        join MEETING
        on 1=1
        join TRAINING
        on 1=1
        join AWAY
        on 1=1
        join AVAILABLE
        on 1=1
        join BREAK
        on 1=1
        """)
        
        
        ALL_Data_2 = ps.sqldf(""" 
        select *, case when OFFLINE is not null then ((((julianday(logout) - julianday(login))  * 86400.0) - OFFLINE)) else  ((((julianday(logout) - julianday(login))  * 86400.0))) end as ONLINE
        from ALL_Data
        """)
        try:
            if len(query_result.results) > 1:
                ALL_Data_2['Avg Talk (Calls)'] = query_result.results[1].data[0].metrics[0].stats.sum / 1000 / query_result.results[1].data[0].metrics[0].stats.count
            else:
                ALL_Data_2['Avg Talk (Calls)'] = query_result.results[0].data[0].metrics[0].stats.sum / 1000 / query_result.results[0].data[0].metrics[0].stats.count
        except:
            ALL_Data_2['Avg Talk (Calls)'] = 0
        try:
            if len(query_result.results) > 1:
                ALL_Data_2['Avg Hold(Calls)'] = query_result.results[1].data[0].metrics[1].stats.sum / 1000 / query_result.results[1].data[0].metrics[1].stats.count
            else:
                ALL_Data_2['Avg Hold(Calls)'] = query_result.results[0].data[0].metrics[1].stats.sum / 1000 / query_result.results[0].data[0].metrics[1].stats.count
        except:
            ALL_Data_2['Avg Hold(Calls)'] = 0
        ALL_Data_2['Date'] = x[:10]
        #ALL_Data_2['Date'] = x[:10]
        
        
        ALL_Data_2['nearest_start'] = ALL_Data_2['LOGIN'].apply(lambda x: nearest_start(x))
        ALL_Data_2['nearest_end'] = ALL_Data_2['LOGIN'].apply(lambda x: nearest_end(x))
    
        ALL_Data_3 = ps.sqldf(""" select *, case when LATENESSs <= 0 then null else LATENESSs end as LATENESS, case when EARLY_LEAVEs <= 0 then null else EARLY_LEAVEs end as EARLY_LEAVE
         from (
        select *, ((((julianday(LOGIN) - julianday(nearest_start))  * 86400.0))) as LATENESSs, ((((julianday(nearest_end) - julianday(LOGOUT))  * 86400.0))) as EARLY_LEAVEs
        from ALL_Data_2) as t
        """)
        #dict3.update(((str(row)).replace(", tzinfo=tzlocal()", "").replace("datetime.datetime","'")).replace(")",")'"))
        #rows_list_2.append(dict2)
        #print()
        ALL_Data_4 = ps.sqldf(""" select Date as Day, 
                              
                                        LOGIN as Login ,
                                        LOGOUT as Logout, 
                                        ONLINE as Online, 
                                        LATENESS as Lateness, 
                                        case when Online >= 32400 then Null else EARLY_LEAVE end as 'Early Leave', 
                                        case when BREAK is null then null else BREAK end as Break,
                                         EXCEEDED_BREAK as 'Exceed Break',
                                         case when AVAILABLE is null then null else AVAILABLE end as Available,
                                         case when OFFLINE is null then null else OFFLINE end as Offline,
                                         case when AWAY is null then null else AWAY end as Away,
                                         case when BUSY is null then null else BUSY end as Busy,
                                         case when MEETING is null then null else MEETING end as Meeting,
                                         case when TRAINING is null then null else TRAINING end as Training,
                                         case when ALL_Data_3.'Avg Talk (Calls)' >= ALL_Data_3.'Avg Hold(Calls)' then ALL_Data_3.'Avg Talk (Calls)' else ALL_Data_3.'Avg Hold(Calls)' end as 'Avg Talk (Calls)', 
                                         case when ALL_Data_3.'Avg Talk (Calls)' >= ALL_Data_3.'Avg Hold(Calls)' then ALL_Data_3.'Avg Hold(Calls)' else ALL_Data_3.'Avg Talk (Calls)' end as 'Avg Hold(Calls)'
                                         from ALL_Data_3""")
    
        ALL_Data_4['Online']   = ALL_Data_4['Online'].apply(lambda x:reformating(x))  
        ALL_Data_4['Lateness']   = ALL_Data_4['Lateness'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Early Leave']   = ALL_Data_4['Early Leave'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Break']   = ALL_Data_4['Break'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Exceed Break']   = ALL_Data_4['Exceed Break'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Available']   = ALL_Data_4['Available'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Offline']   = ALL_Data_4['Offline'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Away']   = ALL_Data_4['Away'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Busy']   = ALL_Data_4['Busy'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Meeting']   = ALL_Data_4['Meeting'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Training']   = ALL_Data_4['Training'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Avg Talk (Calls)']   = ALL_Data_4['Avg Talk (Calls)'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Avg Hold(Calls)']   = ALL_Data_4['Avg Hold(Calls)'].apply(lambda x:reformating(x)) 
        ALL_Data_4['Login']   = ALL_Data_4['Login'].apply(lambda x:split_date(x))
        ALL_Data_4['Logout']   = ALL_Data_4['Logout'].apply(lambda x:split_date(x))
        
    except:
    
        ALL_Data_4 = pd.DataFrame({'Day':get_interval_string_2('Yesterday')[:10], 'Login':None, 'Logout':None, 'Online':None, 'Lateness':None, 'Early Leave':None, 'Break':None,
       'Exceed Break':None, 'Available':None, 'Offline':None, 'Away':None, 'Busy':None, 'Meeting':None,
       'Training':None, 'Avg Talk (Calls)':None, 'Avg Hold(Calls)':None},[0])
        
    return ALL_Data_4

def get_interval_stringv2(follow):
    """Function takes no paramters and returns an time interval in json format"""
    if follow == 'Today':
        yesterday = datetime.now() - timedelta(0)
    if follow == 'Yesterday':
        yesterday = datetime.now() - timedelta(1)
    return yesterday.strftime('%Y-%m-%d') + 'T05:53:01' + '/' + yesterday.strftime('%Y-%m-%d') + 'T23:53:01'



def configure_connection():
    """Function returns the Genesys API client"""
    region = PureCloudPlatformClientV2.PureCloudRegionHosts.eu_west_1
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()
    apiClient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(Gen_Token, Gen_Pass)
    return apiClient

def get_user_information(user_id,apiClient):
    """Function takes the user_id and the api client and returns all the users' information like name, email, etc."""
    user_api = PureCloudPlatformClientV2.UsersApi(apiClient)
    users_data = user_api.get_user(user_id)
    return users_data

def get_queue_information(queue_name,apiClient):
    """Function takes the queue_name and the api client and returns the queue id"""
    routing_api = PureCloudPlatformClientV2.RoutingApi(apiClient)
    queue_data = routing_api.get_routing_queues(name = queue_name)
    queue_id = queue_data.entities[0].id
    return queue_id

def query_builder(metrics_list,dimension,queue_id,Interval,apiClient):
    """Function takes the metrics list, dimension, and queue id and returns the query results generted from the analytics api"""
    #Interval = '2020-11-22T05:53:01/2020-11-22T23:53:01'
    query = PureCloudPlatformClientV2.ConversationAggregationQuery()
    query.interval = Interval
    query.group_by = ['queueId']
    query.metrics = metrics_list
    query.filter = PureCloudPlatformClientV2.ConversationAggregateQueryFilter()
    query.filter.type = 'and'
    query.filter.clauses = [PureCloudPlatformClientV2.ConversationAggregateQueryClause()]
    query.filter.clauses[0].type = 'or'
    query.filter.clauses[0].predicates = [PureCloudPlatformClientV2.ConversationAggregateQueryPredicate()]
    query.filter.clauses[0].predicates[0].dimension = dimension
    query.filter.clauses[0].predicates[0].value = queue_id
    analytics_api = PureCloudPlatformClientV2.AnalyticsApi(apiClient)
    query_result = analytics_api.post_analytics_conversations_aggregates_query(query)
    return query_result

def parsing_query_results(query_result):
    """Function take the results generated from the analytics API then parse it and convert it to dataframe"""
    s = str(query_result)
    lis_1 = s.split(",\n"
    "                                    {'")
    lis_2 = []
    lis_4 = []
    for i in range(3,len(lis_1)-1):
        #Get the metrics
        lis_2.append(((((((lis_1[i][lis_1[i].find("{'count'"): s.find('}},')] + '}').replace('None', '"None"')).replace('\n                                     ',""))).replace('          ','')).replace("\\","").replace('}}','')))
        #Get the metrics names
        lis_4.append(((((((((((lis_1[i][lis_1[i].find("metric':"): s.find(",")] + '}').replace('None', '"None"')).replace('\n                                     ',""))).replace('          ','')).replace("\\","").replace('}}',''))[10:20].replace(',','')).replace('q','')).replace('u','')).replace('"','')).replace("'",''))
    
    lis_3 = []
    rows_list_2 = []
    #Convert String to json
    for i in lis_2:
        lis_3.append(ast.literal_eval(i))
    #Convert json to DataFrame
    for row in lis_3:
        dict2 = {}
        dict2.update(row) 
        rows_list_2.append(dict2)
    #Insert the column with the metrics names
    data_real = pd.DataFrame(rows_list_2)
    data_real['Metric'] = lis_4
    col_name="Metric"
    first_col = data_real.pop(col_name)
    data_real.insert(0, col_name, first_col)
    #Replace None values with so small value to avoid dividing by zero
    data_real[['sum','count','ratio']] = data_real[['sum','count','ratio']].replace("None",0.0000001)
    data_real[['sum','count']] = data_real[['sum','count']].apply(pd.to_numeric)
    #Calculate the average value by dividing the summation by the count
    data_real['avg'] = ((data_real['sum'].div(1000).round(2)) / data_real['count']).round(0)
    return data_real

def get_percentage(df, new_col, col1,col2):
   """Function takes dataframe, new cloumn name, numerator, and denominator columns and returns new dataframe column with the percentage"""
   df[new_col] = (df[col1] / df[col2]+ 0.0000001)
   return df

def transpose_dataframe(data_real):
    """Function takes the dataframe and transpose it"""
    data_real_new =  data_real.T
    new_header = data_real_new.iloc[0] 
    data_real_new = data_real_new[1:] 
    data_real_new.columns = new_header
    data_real_new = data_real_new.iloc[[0,6,-1]]
    df1 = (pd.DataFrame(data_real_new.iloc[0])).T
    df2 = (pd.DataFrame(data_real_new.iloc[1])).T
    df3 = (pd.DataFrame(data_real_new.iloc[2])).T
    full = df1.reset_index(drop=True).merge(df2.reset_index(drop=True), left_index=True, right_index=True)
    full = full.reset_index(drop=True).merge(df3.reset_index(drop=True), left_index=True, right_index=True)
    #full['Transfer %'] = (full['nTransferr'] / full['tAnswered']+ 0.0000001)
    if 'tAbandon' not in full.columns or 'tAcda' not in full.columns:
        full['tAbandon'] = 0
        full['tAcda'] = 0.00001
        full['Abandon %'] = full['tAbandon'] / full['tAcda']
    
             
    else:
        full['Abandon %'] = full['tAbandon'] / full['tAcda']
    return full
def queue_dataframe(queue_name,Interval):

    apiClient = configure_connection()
    id = get_queue_information(queue_name,apiClient)
    
    query_result = query_builder(['tTalkComplete','nBlindTransferred', 'nConnected', 'nConsult', 'nConsultTransferred', 'nError', 'nOffered', 'nOutbound', 'nOutboundAbandoned', 'nOutboundAttempted', 'nOutboundConnected', 'nOverSla', 'nStateTransitionError', 'nTransferred', 'oExternalMediaCount', 'oMediaCount', 'oServiceLevel', 'oServiceTarget', 'tAbandon', 'tAcd', 'tAcw', 'tAgentResponseTime', 'tAlert', 'tAnswered', 'tContacting', 'tDialing', 'tFlowOut', 'tHandle', 'tHeld', 'tHeldComplete', 'tIvr', 'tMonitoring', 'tNotResponding', 'tShortAbandon', 'tTalk', 'tTalkComplete', 'tUserResponseTime', 'tVoicemail', 'tWait'],'queueId',id,Interval,apiClient)
    if query_result.results is None:
        return pd.DataFrame({'Contracts Management':[0],	'Contracts Abandoned':[0],'Contracts Management Abandoned%':[0],	'Customer Experience':[0],	'Customer Abandoned':[0],	'Customer Experience Abandoned%':[0],	'Inbound Calls':[0],	'Total Abandoned':[0],	'Total Abandoned %':[0],	'Outbound Calls':[0],'Service Level %':[0],	'ASA':[0],'Avg Talk':[0],'Avg ACW':[0],'AHT':[0],'Avg Hold':[0]})
    else:
        data_real = parsing_query_results(query_result)
        data_real_new = transpose_dataframe(data_real)
        #data_real_new['tAbandon_x'] / data_real_new['tAcda_x']
        #data_real_new = get_percentage(data_real_new, 'Abandon %', 'tAbandon','tAcda')
        return data_real_new

def merge_queues(df1,df2):
    All_Data = df1.reset_index(drop=True).merge(df2.reset_index(drop=True), left_index=True, right_index=True)
    return All_Data

def query_builder_outbound(user_Ids, metrics, Interval):
    apiClient = configure_connection()
    Ids = user_Ids
    query = PureCloudPlatformClientV2.ConversationAggregationQuery()
    query.interval = Interval
    query.group_by = ['queueId']
    query.metrics = metrics
    query.filter = PureCloudPlatformClientV2.ConversationAggregateQueryFilter()
    query.filter.type = 'and'
    #query.filter.clauses = [PureCloudPlatformClientV2.ConversationAggregateQueryClause()]
    query.filter.type = 'or'
    query.filter.predicates = [PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate(),PureCloudPlatformClientV2.ConversationAggregateQueryPredicate()]
    query.filter.predicates[0].type = 'dimension'
    query.filter.predicates[0].dimension = 'userId'
    query.filter.predicates[0].value = Ids[0]
    query.filter.predicates[0].operator = "matches"
    query.filter.predicates[1].type = 'dimension'
    query.filter.predicates[1].dimension = 'userId'
    query.filter.predicates[1].value = Ids[1]
    query.filter.predicates[1].operator = "matches"
    query.filter.predicates[2].type = 'dimension'
    query.filter.predicates[2].dimension = 'userId'
    query.filter.predicates[2].value = Ids[2]
    query.filter.predicates[2].operator = "matches"
    query.filter.predicates[3].type = 'dimension'
    query.filter.predicates[3].dimension = 'userId'
    query.filter.predicates[3].value = Ids[3]
    query.filter.predicates[3].operator = "matches"
    query.filter.predicates[4].type = 'dimension'
    query.filter.predicates[4].dimension = 'userId'
    query.filter.predicates[4].value = Ids[4]
    query.filter.predicates[4].operator = "matches"
    query.filter.predicates[5].type = 'dimension'
    query.filter.predicates[5].dimension = 'userId'
    query.filter.predicates[5].value = Ids[5]
    query.filter.predicates[5].operator = "matches"
    query.filter.predicates[6].type = 'dimension'
    query.filter.predicates[6].dimension = 'userId'
    query.filter.predicates[6].value = Ids[6]
    query.filter.predicates[6].operator = "matches"
    query.filter.predicates[7].type = 'dimension'
    query.filter.predicates[7].dimension = 'userId'
    query.filter.predicates[7].value = Ids[7]
    query.filter.predicates[7].operator = "matches"
    query.filter.predicates[8].type = 'dimension'
    query.filter.predicates[8].dimension = 'userId'
    query.filter.predicates[8].value = Ids[8]
    query.filter.predicates[8].operator = "matches"
    
    
    analytics_api = PureCloudPlatformClientV2.AnalyticsApi(apiClient)
    query_result = analytics_api.post_analytics_conversations_aggregates_query(query)
    
    if query_result.results is None:
        return [0]
    
    
        
    else:
        s = str(query_result)
        print(s)
        data = '{'+s[s.find("'count") : s.find(",\n                                               'current'")]+'}'
        if(query_result.results[0].group['mediaType'] == 'voice'):
            data = [i for i in query_result.results[0].data[0].metrics if i.metric == 'tContacting'][0].stats.count
        else:
            data = [i for i in query_result.results[1].data[0].metrics if i.metric == 'tContacting'][0].stats.count
        return data

def select_cols(df,lis):
    return df[lis]

def reformating(sec):
    if sec >= 3600:
        hour = round(int(sec / (60*60)),0)
        minute = round((sec - (hour*60*60))/60,0)
        second = round(sec - ((hour*60 + minute) * 60),0)
    elif  sec < 3600 and sec >= 60:
        hour = 0
        minute = round(int(sec / 60),0)
        second = round(sec - (minute * 60),0)
    else:
        hour = 0
        minute = 0
        second = sec

    return str(round(hour))+':'+str(round(minute))+':'+str(round(second))






def upload():
    new_list = []
    full_data_user_Experience = queue_dataframe('Customer Experience',get_interval_stringv2('Yesterday'))
    full_data_contract_Management = queue_dataframe('Contracts Management',get_interval_stringv2('Yesterday'))
    if full_data_user_Experience.columns[0] == 'Contracts Management':
        return full_data_user_Experience
    else:
        All_data = merge_queues(full_data_user_Experience,full_data_contract_Management)
        All_data['outbounds'] = query_builder_outbound(['a4745fac-b854-4d3a-b34c-248f9ee8436d','6f8ea04a-b324-40e4-9e3c-1c6bf1939fed','80d86ba8-7b92-4f81-b388-5848818a738c','8c395417-fd7f-41cb-b700-09f30fee9cb7','a9567b17-a515-49a2-b179-da3d0057e6de','4eac2200-c655-44e0-87de-01d21d7c165f','e465d8a6-e580-4faa-86cf-e82c22040e6a','a1eaa298-2931-4d5e-b351-e6e291dedb29','2eb47365-bd65-40d6-b3a7-820c2a7f1aed'], ['tTalkComplete','nBlindTransferred', 'nConnected', 'nConsult', 'nConsultTransferred', 'nError', 'nOffered', 'nOutbound', 'nOutboundAbandoned', 'nOutboundAttempted', 'nOutboundConnected', 'nOverSla', 'nStateTransitionError', 'nTransferred', 'oExternalMediaCount', 'oMediaCount', 'oServiceLevel', 'oServiceTarget', 'tAbandon', 'tAcd', 'tAcw', 'tAgentResponseTime', 'tAlert', 'tAnswered', 'tContacting', 'tDialing', 'tFlowOut', 'tHandle', 'tHeld', 'tHeldComplete', 'tIvr', 'tMonitoring', 'tNotResponding', 'tShortAbandon', 'tTalk', 'tTalkComplete', 'tUserResponseTime', 'tVoicemail', 'tWait'], get_interval_stringv2(follow='Yesterday'))
        col_list = ['tAcda_x_x', 'tAbandon_x_x','Abandon %_x','tAcda_x_y','tAnswered_x','tAbandon_x_y','Abandon %_y','oServiceLe_y_x','tAcwa_x',	'tHandle_x',	'tHeldCompl_x',	'tTalkCompl_x','outbounds']
        for col in col_list:
            if col not in All_data.columns:
                All_data[col] = [0]
                new_list.append(col)
            else:
                new_list.append(col)
        All_data = select_cols(All_data,new_list)
        All_data.rename(columns={'tAcda_x_x':'Customer Experience','tAbandon_x_x':'Customer Abandoned',
                        'Abandon %_x':'Customer Experience Abandoned%','outbounds':'Outbound Calls',
                         'tAcda_x_y':'Contracts Management','tAbandon_x_y':'Contracts Abandoned',
                         'Abandon %_y':'Contracts Management Abandoned%','oServiceLe_y_x':'Service Level %',                            'tAnswered_x':'ASA','tTalkCompl_x':'Avg Talk', 'tAcwa_x':'Avg ACW',
                         'tHandle_x':'AHT','tHeldCompl_x':'Avg Hold',}, inplace=True)
        All_data['Date'] = get_interval_stringv2('Yesterday')[:10]
        All_data = get_percentage(All_data, 'Contracts Management Abandoned%', 'Contracts Abandoned','Contracts Management')
        All_data = get_percentage(All_data, 'Customer Experience Abandoned%', 'Customer Abandoned','Customer Experience')
        All_data['Inbound Calls'] = All_data['Contracts Management'] + All_data['Customer Experience']
        All_data['Total Abandoned'] = All_data['Contracts Abandoned'] + All_data['Customer Abandoned']
        All_data = get_percentage(All_data, 'Total Abandoned %', 'Total Abandoned','Inbound Calls')
        All_data = select_cols(All_data,['Date','Contracts Management',	'Contracts Abandoned',	'Contracts Management Abandoned%',	'Customer Experience',	'Customer Abandoned',	'Customer Experience Abandoned%',	'Inbound Calls',	'Total Abandoned',	'Total Abandoned %', 'Outbound Calls','Service Level %',	'ASA',	'Avg Talk',	'Avg ACW',	'AHT',	'Avg Hold'])

        All_data['Avg AHT/ Per Minute'] = All_data['AHT'] / 60
        All_data['Avg Hold/ Per Minute'] = All_data['Avg Hold'] / 60
        All_data['Avg ASA/ Per Minute'] = All_data['ASA'] / 60
        All_data['Avg Talk/ Per Minute'] = All_data['Avg Talk'] / 60
        All_data['Avg ACW/ Per Minute'] = All_data['Avg ACW'] / 60
        
        return All_data

   