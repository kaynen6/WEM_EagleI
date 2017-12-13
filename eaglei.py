## This script will get Eagle-I power outage data from DOE and post it to our REST END service for mapping use
## Author Kayne Neigherbauer kayne.n@gmail.com for WI DMA - Wisconsin Emergency Management


try:
    import sys, urllib, urllib2, json, time, sched, string, logging, logging.handlers, traceback, datetime, socket
except ImportError:
    sys.exit("Error importing 1 or more required modules")

def main():

    #function gets data from EAGLE-I REST API
    def getData(url):
        #get data from url
        try:
            webUrl = urllib2.urlopen(url)
        except urllib2.HTTPError as e:
            print "HTTPError Code: " + str(e.code)
            my_logger.exception("func getData HTTP Error: " + e.reason)
        except urllib2.URLError as e:
            print "URLError Code:", str(e.reason)
            my_logger.exception("func getData URL Error code: " + e.reason)
        else:
            code = webUrl.getcode()
            if code == 200:
                #if success (200) then read the data
                try:
                    data = json.load(webUrl)
                except ValueError as e:
                    my_logger.exception(log_time, 'getData Error: %s' + '\n', e)
                if data == []: data = None
                return data
            else:
                return None

    #function to post data to database REST end
    def postData(data, url):
        try:
            req = urllib2.Request(url,'f=json&updates=' + json.dumps(data))
            webUrl = urllib2.urlopen(req)
        except urllib2.HTTPError as e:
            print "http error:", str(e)
            my_logger.exception(log_time,"- postData HTTP Error: " + str(e.code))
        except urllib2.URLError as e:
            print "url error:", str(e)
            my_logger.exception(log_time,"- postData URL Error Code: " + str(e.reason))
        else:
            response = json.load(webUrl)
            #print json.dumps(response, indent=2)
            sCount = 0
            eCount = 0
            if "error" not in response:
                for item in response.get("updateResults"):
                    if item.get("success") == True:
                        sCount += 1
                    else:
                        eCount += 1
                        my_logger.info("Error adding features" + " - " + item['error'].get('description') + str(eCount))
                my_logger.info(str(sCount) + " features successfully added, " + str(eCount) + " errors.")
            else:
                if "message" in response["error"]:
                    my_logger.info("Error adding features" + " - " + response['error'].get('message'))



    #main function that runs on/from the scheduler
    def timedFunc(token,key):
        #define some variables so easily updated at later date if necessary

        #urls
        getCtyUrl = 'https://eagle-i.doe.gov/api/outages/countymax24hoursummary?state=WI&eiApiKey=' + key
        getStUrl = 'https://eagle-i.doe.gov/api/outages/statemax24hoursummary?state=WI&eiApiKey=' + key
        postUrl = 'https://___________________EagleI_Power_Outage/FeatureServer/0/applyEdits?token=' + token
        #get data via function calls
        data = getData(getCtyUrl)
        state = getData(getStUrl)
        if data:
            #match and combine data
            newData = []
            # add state totals data to the county data
            if state:
                stTotCustOut = state["data"][0].get("currentOutage")
            else:
                stTotCustOut = 0
            #format and parse data from eaglei
            for item in countyIDs.iterkeys():
                newItem = {"attributes":{} }
                newItem["attributes"]["County"] = item
                for outage in data["data"]:
                    if item in outage.values():
                        newItem = {"attributes": outage.copy()}
                        newItem["attributes"]["County"] = newItem["attributes"].pop("countyName")
                        break                
                if "currentOutage" not in newItem.get("attributes").keys():
                    newItem["attributes"]["CustomersOut"] = 0
                else:
                    newItem["attributes"]["CustomersOut"] = newItem["attributes"].pop("currentOutage")
                #match county name to odjectID
                newItem["attributes"]["ObjectId"] = countyIDs.get(item)
                newItem["attributes"]["stTotCustOut"] = stTotCustOut
                newData.append(newItem)
                #print newItem
            #post the data to the rest end feature service via function
            postData(newData, postUrl)


    #set a timeout for web requests via socket module
    timeout = 10
    socket.setdefaulttimeout(timeout)
    
    #set up and log file for a logger object
    logFile = 'eaglei.log'
    #log object with a rotating handler
    my_logger = logging.getLogger()
    my_logger.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(logFile,maxBytes = 2*1024*1024,backupCount=2)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt = '%m/%d/%y %I:%M:%S%p')
    handler.setFormatter(formatter)
    my_logger.addHandler(handler)
    
    #get current data and time for logging purposes
    dt = datetime.datetime.now()
    log_time = dt.strftime("%m/%d/%y %I:%M:%S%p")

    #WEM server token
    token = sys.argv[1]
    ##### EagleI api key #####
    key = sys.argv[2]

    global countyIDs
    countyIDs = {"Adams":45, "Ashland":1, "Barron":15, "Bayfield":2, "Brown":33, "Buffalo":37, "Burnett":6, "Calumet":42, "Chippewa":22, "Clark":27, "Columbia":54, "Crawford":60, "Dane":61, "Dodge":56, "Door":18, "Douglas":3, "Dunn":24, "Eau Claire":30, "Florence":10, "Fond du Lac":51, "Forest":9, "Grant":62, "Green":68, "Green Lake":49, "Iowa":64, "Iron":4, "Jackson":39, "Jefferson":66, "Juneau":44, "Kenosha":72, "Kewaunee":31, "La Crosse":48, "Lafayette":71, "Langlade":19, "Lincoln":17, "Manitowoc":41, "Marathon":26, "Marinette":13, "Marquette":50, "Menominee":25, "Milwaukee":63, "Monroe":47, "Oconto":20, "Oneida":12, "Outagamie":40, "Ozaukee":57, "Pepin":32, "Pierce":29, "Polk":14, "Portage":34, "Price":11, "Racine":67, "Richland":59, "Rock":70, "Rusk":16, "Sauk":55, "Sawyer":8, "Shawano":28, "Sheboygan":52, "St. Croix":23, "Taylor":21, "Trempealeau":38, "Vernon":53, "Vilas":5, "Walworth":69, "Washburn":7, "Washington":58, "Waukesha":65, "Waupaca":35, "Waushara":46, "Winnebago": 43, "Wood":36}

    timedFunc(token,key)


if __name__ == "__main__":
    main()
