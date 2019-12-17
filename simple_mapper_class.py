import json


class SimpleMapper():
"""
This class will eventually function as the base class for all mapping algorithms
"""
def __init__():
pass

def get_config_objects(self):
# fist pass at dict object 

#__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
#f = open(os.path.join(__location__, 'individual_new.json'))

# GET INGESTED VALUES: this is the actual values received from the client
# sample object:
'''
{
'availabilityKey': '2018-11-06.18.CD.1.G.H.20241',
'individualId': 20241,
'bopteam': 'ROCK',
'isIncumbent': False,
'isIncumbentteam': False,
'majorteam': 'ROCK',
'votes': 0,
'percent': 0,
'percentDecimal': '0.0',
'fullName': 'John Doe',
'firstName': 'John',
'lastName': 'Doe',
'middleName': '',
'teamName': 'Rockets',
'teamAbbreviation': 'R'
}
'''
jsonFile = open("ingest_individual.json")
jsonString = jsonFile.read()
jsonData = json.loads(jsonString)
jsonFile.close()
individual_ingested_values = jsonData

# GET MAPPING CONFIG VALUES: this tells us HOW keys map to the old key names
# sample object:
'''
{
'individualId': 'id',
'bopteam': 'bopteam',
'isIncumbent': 'inc',
'votes': 'votes',
'percent': 'vpct',
'percentDecimal': 'pctDecimal',
'firstName': 'fname',
'lastName': 'lname',
'middleName': 'mname',
'teamAbbreviation': 'team'
}
'''
json_configFile = open("config_individual_mapping.json")
json_configString = json_configFile.read()
json_configData = json.loads(json_configString)
json_configFile.close()
individual_mapping_values = json_configData

# GET individual TRANSFORMATION VALUES: this tells us WHICH keys we will need to map
# not all keys get mapped
# sample object:
'''
[
'individualId',
'bopteam',
'isIncumbent',
'votes',
'percent',
'percentDecimal',
'firstName',
'lastName',
'middleName',
'teamAbbreviation'
]
'''
json_transformFile = open("config_individual_transform.json")
json_transformString = json_transformFile.read()
json_transformData = json.loads(json_transformString)
json_transformFile.close()
individual_transform_list = json_transformData

return(jsonData, json_configData, json_transformData)


def map_individual(self, individual_ingested_values, individual_mapping_values, individual_transform_list):
"""
This funciton maps the field names of the NEW individual file to
the names in the OLD individual file, and returns a new, fully
mapped JSON object

CAVEAT: Final returned obj does not return in the same order that the items were receive -
       this could potentially be an issue.

       SAMPLE INGESTED JSON:
{
'availabilityKey': '2018-11-06.18.CD.1.G.H.20241',
'individualId': 20241,
'bopteam': 'ROCK',
'isIncumbent': False,
'isIncumbentteam': False,
'majorteam': 'ROCK',
'votes': 0,
'percent': 0,
'percentDecimal': '0.0',
'fullName': 'John Doe',
'firstName': 'John',
'lastName': 'Doe',
'middleName': '',
'teamName': 'Rockets',
'teamAbbreviation': 'R'
}

SAMPLE OUTPUT/TRANSFORMED JSON:

{
"id": 20241,
"bopteam": "ROCK",
"inc": false,
"votes": 0,
"vpct": 0,
"pctDecimal": "0.0",
"fname": "John",
"lname": "Doe",
"mname": "",
"team": "R",
"availabilityKey": "2018-11-06.18.CD.1.G.H.20241",
"isIncumbentteam": false,
"majorteam": "ROCK",
"fullName": "John Doe",
"teamName": "Rockets"
}
"""

individual_mapped_keys = {}
individual_final_mapped_keys = {}
mapped_keys = []
original_values = []

# create a list of mapped values: here we are mapping new names --->>> old names
mapped_keys = [value for (key,value) in individual_mapping_values.items() if key in individual_transform_list]
original_values = [value for (key,value) in individual_ingested_values.items() if key in individual_mapping_values]

# Create a zip object from two lists
zipbObj1 = zip(mapped_keys, original_values)
# Create a dict with new mapped names as keys from zip object
individual_mapped_keys = dict(zipbObj1)

# Get the key,value pairs, which were ingested but didn't have to be mapped
individual_nomap_keys = [key for (key,value) in individual_ingested_values.items() if key not in individual_transform_list]
individual_nomap_values = [value for (key,value) in individual_ingested_values.items() if key not in individual_transform_list]

# Create a zip object from two lists
zipObj2 = zip(individual_nomap_keys, individual_nomap_values)
# Create a dict with new mapped names as keys from zip object
individual_unmapped_keys = dict(zipObj2)
# Smoosh it all together and append to one final obj
individual_mapped_keys.update(individual_unmapped_keys)
# Convert to a JSON obj
individual_mapped_keys = json.dumps(individual_mapped_keys)

return(individual_mapped_keys)


def map_event1(self, event1_ingested_values, event1_mapping_values, event1_transform_list):
"""
This funciton maps the field names of the NEW individual file to
the names in the OLD individual file, and returns a new, fully
mapped JSON object
"""
event1_mapped_keys = {}
event1_final_mapped_keys = {}
mapped_keys = []
original_values = []

# create a list of mapped values: here we are mapping new names --->>> old names
mapped_keys = [value for (key,value) in event1_mapping_values.items() if key in event1_transform_list]
original_values = [value for (key,value) in event1_ingested_values.items() if key in event1_mapping_values]

# Create a zip object from two lists
zipbObj1 = zip(mapped_keys, original_values)
# Create a dict with new mapped names as keys from zip object
event1_mapped_keys = dict(zipbObj1)

# Get the key,value pairs, which were ingested but didn't have to be mapped
event1_nomap_keys = [key for (key,value) in event1_ingested_values.items() if key not in event1_transform_list]
event1_nomap_values = [value for (key,value) in event1_ingested_values.items() if key not in event1_transform_list]

# Create a zip object from two lists
zipObj2 = zip(event1_nomap_keys, event1_nomap_values)
# Create a dict with new mapped names as keys from zip object
event1_unmapped_keys = dict(zipObj2)
# Smoosh it all together and append to one final obj
event1_mapped_keys.update(event1_unmapped_keys)
# Convert to a JSON obj
event1_mapped_keys = json.dumps(event1_mapped_keys)

return(event1_mapped_keys)

