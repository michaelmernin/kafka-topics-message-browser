# Kafka-topic-browser
Restful service exposing kafka topics for browsing  

Supports all major clients (JAVA, POSTMAN, PYTHON, etc)
 
  
### Supported Methods  
base url: http://hostname:port/
##### GET: / (Only applicable when using UI)
returns apps landing page
##### POST : /search
All post requests will be made to this endpoint

### Parameters
| param | type | description | Required | example |
| ------ | ------ | ------ | ----- | ----- |
| searchParam | string | all messages that include this string will be returned    | YES | "searchParam": "900001"
| json_topics | JSONArray<strings> | list of json topic names to be included in search | YES, if including json topics in search | "json_topics": ['example-json-topic']
| avro_topics | JSONArray<strings> | list of avro topic names to be included in search | YES, if including avro topics in search | "avro_topics": ['example-avro-topic']
| includeDelimiter | string | If true, results will include delimiter between messages | NO | "includeDelimiter": "false"
| environment | string | Choose kafka environment | NO, defaults to value in default_constants.py | "environment": "example_environment"



### Making Requests (examples)

#### POSTMAN  
Supports form-data, x-www-form-urlencoded, and raw JSON body requests  
Example Raw JSON body request:
```
{
	"searchParam": "example_string",
	"environment": "example_environment",
	"json_topics":[
		"example-json-topic"
	],
	"avro_topics":[
		"example-avro-topic"
	]
}
```

NOTE: Unless your intention is to send the request via x-www-form-urlencoded, please ensure Content-Type -- application/x-www-form-urlencoded is unchecked, under the 'Headers' tab.  
Sometimes postman will save this setting even though you have selected another content-type option.

#### Java (Rest Assured)
build JSON body
```
JSONObject jsonObject = new JSONObject();  
jsonObject.put("searchParam", "example_string");
JSONArray jsonTopics = new JSONArray();
jsonTopics.add('example-json-topic');
jsonObject.put("json_topics", jsonTopics);  
JSONArray avroTopics = new JSONArray();
avroTopics.add('example-avro-topic');
jsonObject.put("avro_topics", avroTopics);  
String postRequestBody = jsonObject.toString()

Response response = RestAssured.given().config(RestAssured.config().sslConfig(config)).contentType(ContentType.JSON).body(postRequestBody).post(http://<hostname>:<port>>/search);  

JSONObject result = new JSONObject(response.body().asString());  
```

#### Python
Using requests library
```
import requests

URL = "http://<hostname>:<port>/search"
PARAMS = {'searchParam': 'example_string', 'json_topics': ['example-json-topic'],
          'avro_topics': ['example-avro-topic']}
response = requests.post(URL, json=PARAMS)
data = response.json()
```

# Response
List of JSON message objects for each topic included in the search  (example below)
```
{
    "JSON_TOPIC_example-json-topic": [
        {
            "example_response_key": "example_response_value"
        }
    ]
}
```

# Recommended Deployment (Windows Service)  
Recommended deployment as Windows Service
### Windows service details  
- Service Name: <service_name>
- Project Path: <project_path>
- Logs Path: <log_file_path>
- Startup Type: Automatic (will start during the boot process)  
- Service Manager Used: NSSM (https://nssm.cc/)  

### Stopping/Starting service remotely  
Windows Users:
> From command line, run as administrator  
- stop service: ```$ sc \\<hostname> stop <service_name>```  
- start service ```$ sc \\<hostname> start <service_name>``` 

Mac Users: TBD  
