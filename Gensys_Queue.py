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

def get_interval_stringV2v2(follow):
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
    full_data_user_Experience = queue_dataframe('Customer Experience',get_interval_stringV2v2('Yesterday'))
    full_data_contract_Management = queue_dataframe('Contracts Management',get_interval_stringV2v2('Yesterday'))
    if full_data_user_Experience.columns[0] == 'Contracts Management':
        return full_data_user_Experience
    else:
        All_data = merge_queues(full_data_user_Experience,full_data_contract_Management)
        All_data['outbounds'] = query_builder_outbound(['a4745fac-b854-4d3a-b34c-248f9ee8436d','6f8ea04a-b324-40e4-9e3c-1c6bf1939fed','80d86ba8-7b92-4f81-b388-5848818a738c','8c395417-fd7f-41cb-b700-09f30fee9cb7','a9567b17-a515-49a2-b179-da3d0057e6de','4eac2200-c655-44e0-87de-01d21d7c165f','e465d8a6-e580-4faa-86cf-e82c22040e6a','a1eaa298-2931-4d5e-b351-e6e291dedb29','2eb47365-bd65-40d6-b3a7-820c2a7f1aed'], ['tTalkComplete','nBlindTransferred', 'nConnected', 'nConsult', 'nConsultTransferred', 'nError', 'nOffered', 'nOutbound', 'nOutboundAbandoned', 'nOutboundAttempted', 'nOutboundConnected', 'nOverSla', 'nStateTransitionError', 'nTransferred', 'oExternalMediaCount', 'oMediaCount', 'oServiceLevel', 'oServiceTarget', 'tAbandon', 'tAcd', 'tAcw', 'tAgentResponseTime', 'tAlert', 'tAnswered', 'tContacting', 'tDialing', 'tFlowOut', 'tHandle', 'tHeld', 'tHeldComplete', 'tIvr', 'tMonitoring', 'tNotResponding', 'tShortAbandon', 'tTalk', 'tTalkComplete', 'tUserResponseTime', 'tVoicemail', 'tWait'], get_interval_stringV2v2(follow='Yesterday'))
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
        All_data['Date'] = get_interval_stringV2v2('Yesterday')[:10]
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

   