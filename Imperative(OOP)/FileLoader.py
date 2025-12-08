import csv
import json

class FileLoader : 
    def __init__(self,input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.ext = self.input_path.lower().split('.')[-1]

    def load(self):
        
        if self.ext == 'csv':
            with open(self.input_path, 'r') as file:
                reader = csv.DictReader(file)
                return list(reader)
        elif self.ext == 'json':
            with open(self.input_path, 'r') as file:
                data = json.load(file)
                return list(data)
        else:
            raise ValueError("Unsupported file format. Use CSV or JSON.")
        
    def save(self, data):
        if self.ext == 'csv':
            with open(self.output_path, 'w', newline='') as file:
                if len(data) == 0:
                    return
                writer = csv.DictWriter(file, fieldnames=list(data[0].keys()))
                writer.writeheader()
                writer.writerows(data)
        elif self.ext == 'json':
            with open(self.output_path, 'w') as file:
                json.dump(data, file, indent=4)
        else:
            raise ValueError("Unsupported file format. Use CSV or JSON.")