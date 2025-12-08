from datetime import datetime

class DataStandardizer :
    def __init__(self):
        # Allowed date formats to try
        self.date_formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y/%m/%d'
        ]

    def standardize_numeric_column(self, data, column):
        """
        Convert all values in the column to float (rounded to 2 decimals).
        Invalid or missing values become None.
        """
        for row in data:
            try:
                row[column] = round(float(row[column]), 2)
            except (ValueError, TypeError, KeyError):
                row[column] = None
        return data

    def standardize_categorical_column(self, data, column):
        for row in data:
            val = row.get(column, "Unknown")
            row[column] = str(val).strip()
        return data

    def standardize_date_column(self, data, column):
        for row in data:
            raw = row.get(column)

            if not raw:
                row[column] = None
                continue

            parsed_successfully = False

            for fmt in self.date_formats:
                try:
                    parsed = datetime.strptime(raw, fmt)
                    row[column] = parsed.strftime('%Y-%m-%d')
                    parsed_successfully = True
                    break
                except ValueError:
                    continue

            if not parsed_successfully:
                row[column] = None

        return data
        """Parse date into standard '%Y-%m-%d'. If invalid, set None."""
        raw = row.get(column)

        if not raw:
            row[column] = None
            return row

        for fmt in self.date_formats:
            try:
                parsed = datetime.strptime(raw, fmt)
                row[column] = parsed.strftime('%Y-%m-%d')
                return row
            except ValueError:
                continue

        # No format matched
        row[column] = None
        return row