import pandas as pd
import urllib
def AppsFlyerApi(api_endpoint,api_token,app_id,report_name,from_date,to_date):

    query_params = {
        "api_token": api_token,
        "from": str(from_date),
        "to": str(to_date)}

#https://hq.appsflyer.com/export/id1113492041/partners_report/v5?api_token={Account owner API key should be used}&from=yyyy-mm-dd&to=yyyy-mm-dd&timezone=Africa%2fCairo
#https://hq.appsflyer.com/export/id1113492041/organic_in_app_events_report/v5?api_token={Account owner API key should be used}&from=yyyy-mm-dd&to=yyyy-mm-dd&timezone=Africa%2fCairo&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,amazon_aid,keyword_match_type,att,conversion_type,campaign_type&maximum_rows=1000000
    query_string = urllib.parse.urlencode(query_params)
    

    request_url = api_endpoint + app_id + "/" +  report_name + "/v5?" + query_string

    resp = urllib.request.urlopen(request_url)

    with open("data.csv","wb") as fl:
        fl.write(resp.read())
        
    return pd.read_csv("data.csv")