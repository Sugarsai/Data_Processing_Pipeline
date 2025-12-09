from itertools import groupby
from operator import itemgetter

class DataTransformer:
    def __init__(self):
        pass


    def filter_rows(self, data, col, threshold):
        filtered = []
        for row in data:
            val = row.get(col)
            if val is not None and val > threshold:
                filtered.append(row)
        return filtered


    def aggregate(self, data, group_col, value_col):
        aggregates = {}
        for row in data:
            key = row.get(group_col, "Unknown")
            val = row.get(value_col)
            if val is None:
                continue
            aggregates[key] = aggregates.get(key, 0) + val
        return aggregates
