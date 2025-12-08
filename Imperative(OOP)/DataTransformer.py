from itertools import groupby
from operator import itemgetter

class DataTransformer:
    def __init__(self):
        pass


    def filter_rows(self, data, col, threshold):
        """
        Returns only rows where data[col] > threshold.
        """
        filtered = []
        for row in data:
            val = row.get(col)
            if val is not None and val > threshold:
                filtered.append(row)
        return filtered


        """
        Computes sequential growth percentage per group.
        """
        # Sort data by group_col and optionally by another column (like date)
        sorted_data = sorted(data, key=itemgetter(group_col))

        # Group by group_col
        grouped = groupby(sorted_data, key=itemgetter(group_col))
        result = []

        for key, group_iter in grouped:
            prev_val = None
            for row in group_iter:
                new_row = dict(row)
                val = new_row.get(value_col)
                if val is None:
                    new_row[growth_col] = None
                elif prev_val is None or prev_val == 0:
                    new_row[growth_col] = 0.0
                    prev_val = val
                else:
                    growth = (val - prev_val) / prev_val * 100
                    new_row[growth_col] = round(growth, 2)
                    prev_val = val
                result.append(new_row)
        return result

    def aggregate(self, data, group_col, value_col):
        aggregates = {}
        for row in data:
            key = row.get(group_col, "Unknown")
            val = row.get(value_col)
            if val is None:
                continue
            aggregates[key] = aggregates.get(key, 0) + val
        return aggregates
