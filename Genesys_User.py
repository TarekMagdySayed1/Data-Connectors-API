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

def get_interval_string(follow):
        """Function takes no paramters and returns an time interval in json format"""
        if follow == 'Yesterday':
            yesterday = datetime.now() - timedelta(1)
       
        return yesterday.strftime('%Y-%m-%d') + 'T03:00:01' + '/' + yesterday.strftime('%Y-%m-%d') + 'T23:30:00', yesterday.strftime('%Y-%m-%d') + 'T03:00:01' + '/' + yesterday.strftime('%Y-%m-%d') + 'T23:30:00', yesterday.strftime('%Y-%m-%d') + 'T03:00:01' + '/' + yesterday.strftime('%Y-%m-%d') + 'T23:30:00'
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
    apiClient = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(Gen_Token , Gen_Pass)
    api_instance = PureCloudPlatformClientV2.AnalyticsApi(apiClient)
    body = PureCloudPlatformClientV2.UserDetailsQuery()
    x,y,z = get_interval_string('Yesterday')
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
    body_2 = {
      "interval": y,
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
    "pageNumber": 2
  }
      
      
      
    }
    
    body_3 = {
      "interval": z,
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
    "pageNumber": 3
  }
      
    }
    
    try:
        api_response = api_instance.post_analytics_users_details_query(body)
        api_response_2 = api_instance.post_analytics_users_details_query(body_2)
        api_response_3 = api_instance.post_analytics_users_details_query(body_3)
        
       
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
            try:
                for row in api_response_2.user_details[0].primary_presence:
                    #print(row)
                    lst_2.append(ast.literal_eval(((str(row)).replace(", tzinfo=tzutc()", "").replace("datetime.datetime","'")).replace(")",")'").replace('\n','').replace("None", "'None'")))
                    #print(ast.literal_eval(((str(row)).replace(", tzinfo=tzutc()", "").replace("datetime.datetime","'")).replace(")",")'").replace('\n','').replace("None", "'None'")))
                Abanoub_2 = pd.DataFrame(lst_2)
                #print(Abanoub_2)
                Abanoub_2['start_time'] = Abanoub_2['start_time'].apply(lambda x:convert(x))
                Abanoub_2['end_time'] = Abanoub_2['end_time'].apply(lambda x:convert(x))
                Abanoub_2['start_time'] = pd.to_datetime(Abanoub_2['start_time'])
                Abanoub_2['end_time'] = pd.to_datetime(Abanoub_2['end_time'])
                Abanoub_2['start_time'] = Abanoub_2['start_time'].fillna(pd.to_datetime('2015-01-01'))
                Abanoub_2['end_time'] = Abanoub_2['end_time'].fillna(pd.to_datetime('2015-01-01'))
                Abanoub_2['start_time'] = Abanoub_2['start_time'].dt.tz_localize('Etc/GMT')
                Abanoub_2['end_time'] = Abanoub_2['end_time'].dt.tz_localize('Etc/GMT')
                Abanoub_2['end_time'] = Abanoub_2['end_time'].dt.tz_convert('Etc/GMT')
                Abanoub_2['start_time'] = Abanoub_2['start_time'].dt.tz_convert('Etc/GMT-2')
                Abanoub_2['end_time'] = Abanoub_2['end_time'].dt.tz_convert('Etc/GMT-2')
                print(len(Abanoub_2))
    
                Abanoub = ps.sqldf(""" select *
                                    from Abanoub
                                    union 
                                    select *
                                    from Abanoub_2
                                    
                                    """)
            except:
                Abanoub = Abanoub
            try:
                for row in api_response_3.user_details[0].primary_presence:
                    #print(row)
                    lst_3.append(ast.literal_eval(((str(row)).replace(", tzinfo=tzutc()", "").replace("datetime.datetime","'")).replace(")",")'").replace('\n','').replace("None", "'None'")))
                    #print(ast.literal_eval(((str(row)).replace(", tzinfo=tzutc()", "").replace("datetime.datetime","'")).replace(")",")'").replace('\n','').replace("None", "'None'")))
                Abanoub_3 = pd.DataFrame(lst_3)
                #print(Abanoub_2)
                Abanoub_3['start_time'] = Abanoub_3['start_time'].apply(lambda x:convert(x))
                Abanoub_3['end_time'] = Abanoub_3['end_time'].apply(lambda x:convert(x))
                Abanoub_3['start_time'] = pd.to_datetime(Abanoub_3['start_time'])
                Abanoub_3['end_time'] = pd.to_datetime(Abanoub_3['end_time'])
                Abanoub_3['start_time'] = Abanoub_3['start_time'].fillna(pd.to_datetime('2015-01-01'))
                Abanoub_3['end_time'] = Abanoub_3['end_time'].fillna(pd.to_datetime('2015-01-01'))
                Abanoub_3['start_time'] = Abanoub_3['start_time'].dt.tz_localize('Etc/GMT')
                Abanoub_3['end_time'] = Abanoub_3['end_time'].dt.tz_localize('Etc/GMT')
                Abanoub_3['end_time'] = Abanoub_3['end_time'].dt.tz_convert('Etc/GMT')
                Abanoub_3['start_time'] = Abanoub_3['start_time'].dt.tz_convert('Etc/GMT-2')
                Abanoub_3['end_time'] = Abanoub_3['end_time'].dt.tz_convert('Etc/GMT-2')
                print(len(Abanoub_3))
    
                Abanoub = ps.sqldf(""" select *
                                    from Abanoub
                                    union 
                                    select *
                                    from Abanoub_3
                                    
                                    """)
            except:
                Abanoub = Abanoub
            
    
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

