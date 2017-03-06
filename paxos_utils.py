import json

def json_spaceless_dump(obj):
	json.dumps(obj, separators=(',', ':'))