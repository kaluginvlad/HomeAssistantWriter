import json
import string

class Device:
    def __init__(self, config):
        # Load configuration
        config = json.loads(config)

        self.name = config['name']
        self.dev_class = config['device_class']
        self.state_topic = config['state_topic']
        self.id = config['device']['identifiers'][0]
        self.manufacturer = config['device']['manufacturer']
        self.model = config['device']['model']
        self.name = config['device']['name']
        self.value_template = config['value_template']

        if self.dev_class != "plug":
            self.unit = config['unit_of_measurement']
        else:
            self.on = config['payload_on']
            self.off = config['payload_off']

        if "value_json" not in self.value_template:
            raise Exception("Writer support only JSON data")

        # Extract json key
        key_start = self.value_template.find("value_json.") + 11
        key_end = self.value_template.find(" ", key_start)
        self.json_key = self.value_template[key_start:key_end]

        self.filter_topic = "".join(self.state_topic.split("/")[2:])
    
    def get_data(self, payload):
        state_data = json.loads(payload)[self.json_key]

        if self.dev_class == "plug":
            if state_data == self.on:
                return 1
            return 0

        if type(state_data) == str:
            state_data = state_data.replace(",", ".")
            if state_data == "On":
                state_data = 1
            if state_data == "Off":
                state_data = 0

        return float(state_data)