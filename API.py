
from flask import Flask,jsonify
import pandas as pd
import requests
import  time
from datetime import datetime, timedelta
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
from freshdeskv5 import API
from appsflyer import AppsFlyerApi
from Genesys import KPIs
from Genesys import upload


#freshchatKey = os.environ['FreshChat_KEY']

#!pip install flask_ngrok
#from flask_ngrok import run_with_ngrok

def get_interval_string_fresh(follow):
        """Function takes no paramters and returns an time interval in json format"""
        if follow == 0:
            yesterday = datetime.now() - timedelta(0)
            s = datetime.now() - timedelta(1)
        if follow == 1:
            yesterday = datetime.now() - timedelta(1)
            s = datetime.now() - timedelta(2)
        return yesterday.strftime('%Y-%m-%d') + 'T22:00:00.000Z' , s.strftime('%Y-%m-%d') + 'T22:00:00.000Z'
def get_interval_string_apps(follow):
        """Function takes no paramters and returns an time interval in json format"""
        if follow == 0:
            yesterday = datetime.now() - timedelta(0)
            
        if follow == 1:
            yesterday = datetime.now() - timedelta(1)
    
        return yesterday.strftime('%Y-%m-%d')

def get_interval_string_freshdesk(follow):
    """Function takes no paramters and returns an time interval in json format"""
    if follow == 'Today':
        yesterday = datetime.now() - timedelta(0)
        s = datetime.now() - timedelta(1)
        r = datetime.now() - timedelta(2)
    if follow == 'Yesterday':
        yesterday = datetime.now() - timedelta(18)
        s = datetime.now() - timedelta(19)
        r = datetime.now() - timedelta(20)
    
    return yesterday.strftime('%Y-%m-%d') + 'T22:00:00.000Z' , s.strftime('%Y-%m-%d') + 'T22:00:00.000Z', r.strftime('%Y-%m-%d') + 'T22:00:00.000Z'

z,y,x = get_interval_string_freshdesk('Yesterday')
yesterday = get_interval_string_apps(1)
Freshdesk_URL = 'https://moneyfellows.freshchat.com/v2/reports/raw/'
Freshdesk_Key = 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJpS216TTVkenRIWmprdmdSY3VrVHgxTzJ2SFlTM0U5YmVJME9XbXRNR1ZzIn0.eyJqdGkiOiI1MDMyNDYxMS0yZjViLTRjOWEtOGYxNS1kY2JlM2RiNjU2OTMiLCJleHAiOjE5MjI4MDk3NjAsIm5iZiI6MCwiaWF0IjoxNjA3NDQ5NzYwLCJpc3MiOiJodHRwOi8vaW50ZXJuYWwtZmMtdXNlMS0wMC1rZXljbG9hay1vYXV0aC0xMzA3MzU3NDU5LnVzLWVhc3QtMS5lbGIuYW1hem9uYXdzLmNvbS9hdXRoL3JlYWxtcy9wcm9kdWN0aW9uIiwiYXVkIjoiMjVkMGNiNWEtZTRiMy00Yzk3LWE0MGUtMzcxNWQ4OWVjYTFiIiwic3ViIjoiOWRlNTc2ODctZDFhNC00ODQ4LTgyYjQtYTIzZDUwODllZDM5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiMjVkMGNiNWEtZTRiMy00Yzk3LWE0MGUtMzcxNWQ4OWVjYTFiIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiOTI5NzMyYWUtYmEzMS00ZWE5LWJmZGItZjYwNDczNzI0ZGUzIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6W10sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJhZ2VudDp1cGRhdGUgYWdlbnQ6Y3JlYXRlIG1lc3NhZ2U6Y3JlYXRlIG1lc3NhZ2U6Z2V0IHVzZXI6ZGVsZXRlIGZpbHRlcmluYm94OmNvdW50OnJlYWQgdXNlcjp1cGRhdGUgcm9sZTpyZWFkIGltYWdlOnVwbG9hZCBiaWxsaW5nOnVwZGF0ZSByZXBvcnRzOmV4dHJhY3QgY29udmVyc2F0aW9uOnJlYWQgZGFzaGJvYXJkOnJlYWQgcmVwb3J0czpleHRyYWN0OnJlYWQgcmVwb3J0czpyZWFkIGFnZW50OnJlYWQgZmlsdGVyaW5ib3g6cmVhZCBjb252ZXJzYXRpb246dXBkYXRlIGNvbnZlcnNhdGlvbjpjcmVhdGUgYWdlbnQ6ZGVsZXRlIG91dGJvdW5kbWVzc2FnZTpnZXQgb3V0Ym91bmRtZXNzYWdlOnNlbmQgdXNlcjpjcmVhdGUgcmVwb3J0czpmZXRjaCB1c2VyOnJlYWQiLCJjbGllbnRIb3N0IjoiMTkyLjE2OC4xMjkuMjQ5IiwiY2xpZW50SWQiOiIyNWQwY2I1YS1lNGIzLTRjOTctYTQwZS0zNzE1ZDg5ZWNhMWIiLCJjbGllbnRBZGRyZXNzIjoiMTkyLjE2OC4xMjkuMjQ5In0.ooPrVJ2e_ljLbQYm5Vu7TxickrrZjXXtPi0ciUMhB12O-3xow7B5iDJEmM5l_H5RK9iSXwhZQTOg5TEYoSwJ5vDH4sh1n5iE53ATCqIUMWdH-p0Xv5TsJXI9B7avVBAL9ZRJuv5KwCIMV_kr5F91b2bBOKbp3UwCFlGPJCNUSblBZEsxvkRK01-2Vk0xkMJglGRIosJ4HqHjL5UqFZtNRl6QWqSEO0pcGKG7VtVG85mK3NYcVhhsNnNYZ106W3bdIbaP_JNU_yDhiPiqpaBbkypXvxRVI9F5yZ5xNQeNjf9vXBV4HpcDiK6Dz9piF7SZzGz3-DVoC0i2MpT90Yf6tw'


application = Flask(__name__)
@application.route('/', methods=['GET'])
def home():
	return "<h1>Data Connectors API</h1><p>This site is an API for the data connectors project.</p>"


@application.route('/freshdesk/Tickets')
def Tickets():
    a = API('moneyfellows.freshdesk.com', 'oH1IwmqZS7xH6tdGlW4p')
    rows_list = []
    #query = "(status:2 OR status:'3' OR status:'4' OR status:'5' )"
    s = a.tickets.list_tickets(filter_name=None, updated_since= z)
    try:
        for row in s:
        
                dict1 = {}
                dict1.update(row) 
                rows_list.append(dict1)
        
        data_api = pd.DataFrame(rows_list)
        res = data_api.to_dict(orient='records')
        res = jsonify(res)
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/freshdesk/Contacts')
def Contacts():
    a = API('moneyfellows.freshdesk.com', 'oH1IwmqZS7xH6tdGlW4p')
    rows_list = []
    #query = "(status:2 OR status:'3' OR status:'4' OR status:'5' )"
    s = a.contacts.list_contacts(filter_name=None, _updated_since= x)
    try:
        for row in s:
        
                dict1 = {}
                dict1.update(row) 
                rows_list.append(dict1)
        
        data_api = pd.DataFrame(rows_list)
        res = data_api.to_dict(orient='records')
        res = jsonify(res)
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/freshdesk/Groups')
def Groups():
    a = API('moneyfellows.freshdesk.com', 'oH1IwmqZS7xH6tdGlW4p')
    rows_list = []
    #query = "(status:2 OR status:'3' OR status:'4' OR status:'5' )"
    s = a.groups.list_groups()
    try:
        for row in s:
        
                dict1 = {}
                dict1.update(row) 
                rows_list.append(dict1)
        
        data_api = pd.DataFrame(rows_list)
        res = data_api.to_dict(orient='records')
        res = jsonify(res)
    except:
        res  = jsonify({'result':'Empty'})
    return res
@application.route('/freshdesk/Agents')
def Agents():
    a = API('moneyfellows.freshdesk.com', 'oH1IwmqZS7xH6tdGlW4p')
    rows_list = []
    #query = "(status:2 OR status:'3' OR status:'4' OR status:'5' )"
    s = a.agents.list_agents()
    try:
        for row in s:
        
                dict1 = {}
                dict1.update(row) 
                rows_list.append(dict1)
        
        data_api = pd.DataFrame(rows_list)
        res = data_api.to_dict(orient='records')
        res = jsonify(res)
    except:
        res  = jsonify({'result':'Empty'})
    return res


@application.route('/freshdesk/Comments')
def Comments():
    rows_list = []
    rows_list_2 = []
    a = API('moneyfellows.freshdesk.com', 'oH1IwmqZS7xH6tdGlW4p')
    s = a.tickets.list_tickets(filter_name=None, updated_since= z)
    try:
        for row in s:
        
                dict1 = {}
                dict1.update(row) 
                rows_list.append(dict1)
        
        data_api = pd.DataFrame(rows_list)
        rows_list = []
        for i in list(data_api['id']):
            
            m =  a.comments.list_comments(i)
            time.sleep(1)
            for row in m:
                dict1 = {}
                dict1.update(row) 
                rows_list_2.append(dict1)
        
        ticketlist_api = pd.DataFrame(rows_list_2)
        res = ticketlist_api.to_dict(orient='records')
        res = jsonify(res)
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/appsflyer/In-app-events')
def Events():
    try:
        ios_non_organic_in_app = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "in_app_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_non_organic_in_app)
        ios_organic_in_app = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "organic_in_app_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_organic_in_app)
        android_non_organic_in_app = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "in_app_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_non_organic_in_app)
        android_organic_in_app = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "organic_in_app_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_organic_in_app)
        full_data = pd.concat([ios_non_organic_in_app, ios_organic_in_app,android_non_organic_in_app,android_organic_in_app], ignore_index=True)
        res = full_data.to_dict(orient='records')
        res = jsonify(res) 
    except:
        res  = jsonify({'result':'Empty'})
    return res
@application.route('/appsflyer/Installs')
def Installs():
    try:
        ios_non_organic_installs = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "installs_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_non_organic_installs)
        ios_organic_installs = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "organic_installs_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_organic_installs)
        android_non_organic_installs = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "installs_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_non_organic_installs)
        android_organic_installs = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "organic_installs_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_organic_installs)
        full_data = pd.concat([ios_non_organic_installs, ios_organic_installs,android_non_organic_installs,android_organic_installs], ignore_index=True)
        res = full_data.to_dict(orient='records')
        res = jsonify(res) 
    except:
        res  = jsonify({'result':'Empty'})
    return 

@application.route('/appsflyer/Uninstalls')
def Uninstalls():
    try:
        ios_non_organic_uninstalls = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "uninstall_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_non_organic_uninstalls)
        ios_organic_uninstalls = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "id1113492041"   ,     
        report_name = "organic_uninstall_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(ios_organic_uninstalls)
        android_non_organic_uninstalls = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "uninstall_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_non_organic_uninstalls)
        android_organic_uninstalls = AppsFlyerApi(api_endpoint = "https://hq.appsflyer.com/export/",
        api_token = "a0e8dc8b-8c1c-4131-b9b0-cd70b458da6b" ,
        app_id = "com.moneyfellows.mobileapp"   ,     
        report_name = "organic_uninstall_events_report",
        from_date =  yesterday,
        to_date = yesterday)
        print(android_organic_uninstalls)
        full_data = pd.concat([ios_non_organic_uninstalls, ios_organic_uninstalls,android_non_organic_uninstalls,android_organic_uninstalls], ignore_index=True)
        res = full_data.to_dict(orient='records')
        res = jsonify(res) 
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/Genesys/Users details')
def User_Details():
    try:
        Abanoub_Nabil = KPIs('a4745fac-b854-4d3a-b34c-248f9ee8436d')
        Abanoub_Nabil['name'] = 'Abanoub Nabil'
        Huda_Aladdin = KPIs('6f8ea04a-b324-40e4-9e3c-1c6bf1939fed')
        Huda_Aladdin['name'] = 'Huda Aladdin'
        Mai_Mansour = KPIs('80d86ba8-7b92-4f81-b388-5848818a738c')
        Mai_Mansour['name'] = 'Mai Mansour'
        Hadier_Mahgoub = KPIs('8c395417-fd7f-41cb-b700-09f30fee9cb7')
        Hadier_Mahgoub['name'] = 'Hadier Mahgoub'
        Aya_Reda = KPIs('a9567b17-a515-49a2-b179-da3d0057e6de')
        Aya_Reda['name'] = 'Aya Reda'
        Fakher_Mahdy = KPIs('4eac2200-c655-44e0-87de-01d21d7c165f')
        Fakher_Mahdy['name'] = 'Fakher Mahdy'
        Sarah_Magdi = KPIs('e465d8a6-e580-4faa-86cf-e82c22040e6a')
        Sarah_Magdi['name'] = 'Sarah Magdi'
        Aliaa_Khaled = KPIs('a1eaa298-2931-4d5e-b351-e6e291dedb29')
        Aliaa_Khaled['name'] = 'Aliaa Khaled'
        Ibrahim_Samir = KPIs('2eb47365-bd65-40d6-b3a7-820c2a7f1aed')
        Ibrahim_Samir['name'] = 'Ibrahim Samir'
        
        full_data = pd.concat([Abanoub_Nabil, Huda_Aladdin,Mai_Mansour,Hadier_Mahgoub,Aya_Reda,Aya_Reda,Fakher_Mahdy,Sarah_Magdi,Aliaa_Khaled,Ibrahim_Samir], ignore_index=True)
        res = full_data.to_dict(orient='records')
        res = jsonify(res) 
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/Genesys/Queue details')
def Queue_Details():
    try:
        full_data = upload()
        
        res = full_data.to_dict(orient='records')
        res = jsonify(res) 
    except:
        res  = jsonify({'result':'Empty'})
    return res

@application.route('/freshchat/Conversation-Created')
def Conversation_Created():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Created","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Conversation-Created/<int:day>')
def Conversation_Created_day(day):
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(day)
    print(day)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Created","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    return jsonify(res)
@application.route('/freshchat/Conversation-Agent-Assigned')
def Conversation_Agent_Assigned():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Agent-Assigned","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Conversation-Agent-Assigned/<int:day>')
def Conversation_Agent_Assigned_day(day):
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(day)
    print(day)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Agent-Assigned","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    return jsonify(res)

@application.route('/freshchat/Conversation_Resolved')
def Conversation_Resolved():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Resolved","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)
@application.route('/freshchat/Message-Sent')
def Message_Sent():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Message-Sent","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Conversation-Group-Assigned')
def Conversation_Group_Assigned():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Conversation-Group-Assigned","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Agent-Activity')
def Agent_Activity():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Agent-Activity","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)
@application.route('/freshchat/First-Response-Time')
def First_Response_Time():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"First-Response-Time","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Response-Time')
def Response_Time():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Response-Time","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Resolution-Time')
def Resolution_Time():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Resolution-Time","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)

@application.route('/freshchat/Agent-Intelliassign-Activity')
def Agent_Intelliassign_Activity():
    url = Freshdesk_URL
    API_KEY =Freshdesk_Key
    session = requests.Session()
    session.headers['Authorization'] = API_KEY
    session.headers['Accept'] = "application/json"
    session.headers['Content-Type'] = "application/json"
    end,start = get_interval_string_fresh(1)

    response = session.post(url,data = ("""{"start": """ + (start) + """ ,"end": """ +(end) + """ ,"event":"Agent-Intelliassign-Activity","format":"csv"}""").replace(' ','"'))
    s = str(response.content)
    k = s[s.find('w/'): s.find('"}')].replace('w/','')


    time.sleep(30)
    url = Freshdesk_URL + k
    response = session.get(url)
    m = str(response.content)
    print(m)
    link = m[m.find('href":"'): m.find('"}')].replace('href":"','')
    resp = urlopen(link)
    zipfile = ZipFile(BytesIO(resp.read()))
    try:
        df1 = pd.read_csv(zipfile.open(zipfile.namelist()[0]))
        res = df1.to_dict(orient='records')
    except:
        res  = jsonify({'result':'Empty'})
    
    return jsonify(res)



        
    
if __name__ == "__main__":
 application.run()