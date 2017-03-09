import json, pickle

message_template = "Message {}"

class PythonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return json.JSONEncoder.default(self, obj)
        return {'_python_object': pickle.dumps(obj)}

def as_python_object(dct):
    if '_python_object' in dct:
        return pickle.loads(str(dct['_python_object']))
    return dct

def json_spaceless_dump(obj):
	return json.dumps(obj, separators=(',', ':'), cls=PythonObjectEncoder)

def json_set_serializable_load(obj):
	return json.loads(obj, object_hook=as_python_object)

def command_generator(num_message):
	commands = []
	for i in xrange(num_message):
		commands.append(message_template.format(str(i)))
	return commands