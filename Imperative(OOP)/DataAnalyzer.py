
import statistics
from scipy.stats import linregress

class DataAnalyzer :
    def __init__(self):
        pass
  

    def summary(self, data, col):
        values = [row.get(col) for row in data ]
        clean = [v for v in values if v is not None]
        if not clean:
            return None

        return {
            "count": len(clean),
            "mean": statistics.mean(clean),
            "median": statistics.median(clean),
            "variance": statistics.variance(clean) if len(clean) > 1 else 0,
            "min": min(clean),
            "max": max(clean)
        }

    def correlation(self, data, col1, col2):
        x_values = [row.get(col1) for row in data]
        y_values = [row.get(col2) for row in data]

        clean_pairs = [(x, y) for x, y in zip(x_values, y_values)
                       if x is not None and y is not None]

        if len(clean_pairs) < 2:
            return None

        xs, ys = zip(*clean_pairs)
        return statistics.correlation(xs, ys)

    def correlation_matrix(self, data, columns):
        matrix = {}
        for col1 in columns:
            matrix[col1] = {}
            for col2 in columns:
                matrix[col1][col2] = self.correlation(data, col1, col2)
        return matrix

    def trend(self, data, x_col, y_col):
        x_values = [row.get(x_col) for row in data]
        y_values = [row.get(y_col) for row in data]

        clean_pairs = [(x, y) for x, y in zip(x_values, y_values)
                       if x is not None and y is not None]

        if len(clean_pairs) < 2:
            return None

        xs, ys = zip(*clean_pairs)
        model = linregress(xs, ys)

        return {
            "slope": model.slope,
            "intercept": model.intercept,
            "r_value": model.rvalue,
            "p_value": model.pvalue
        }