class DataValidator:
    def __init__(self):
        pass
    def remove_none_keys(self, data):
        return [{k: v for k, v in row.items() if k is not None} for row in data]
