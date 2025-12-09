import statistics
from collections import Counter
class MissingDataHandler :
    def __init__(self):
        pass
      
    def detect_missing(self, data):
        missing_info = []
        missing_stat = {}
        for i, record in enumerate(data):
            for key, value in record.items():
                if value in [None, '', 'NA', 'N/A']:
                    missing_info.append((i, key))
                    missing_stat[key] = missing_stat.get(key, 0) + 1
        return missing_info, missing_stat
    
    def impute_mean(self, data, column):
        values = [float(row[column]) for row in data if row.get(column) not in (None, '', 'NA')]
        if not values:
            return data
        mean_val = statistics.mean(values)
        for row in data:
            if row.get(column) in (None, '', 'NA'):
                row[column] = round(mean_val, 2)
        return data

    def impute_median(self, data, column):
        values = [float(row[column]) for row in data if row.get(column) not in (None, '', 'NA')]
        if not values:
            return data
        median_val = statistics.median(values)
        for row in data:
            if row.get(column) in (None, '', 'NA'):
                row[column] = median_val
        return data

    def impute_mode(self, data, column):
        values = [row[column] for row in data if row.get(column) not in (None, '', 'NA')]
        if not values:
            return data
        try:
            mode_val = statistics.mode(values)
        except statistics.StatisticsError:
            # If no unique mode, choose most common manually
            mode_val = Counter(values).most_common(1)[0][0]
        for row in data:
            if row.get(column) in (None, '', 'NA'):
                row[column] = mode_val
        return data

    def fill_default(self, data, column, default_value):
        for row in data:
            if row.get(column) in (None, '', 'NA'):
                row[column] = default_value
        return data

    def drop_missing(self, data, columns=None):
        if columns is None:
            columns = data[0].keys() if data else []

        filtered = []
        for row in data:
            if all(row.get(col) not in (None, '', 'NA') for col in columns):
                filtered.append(row)
        return filtered
