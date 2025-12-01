import sys
import csv
import json
from datetime import datetime
import statistics
from functools import reduce
import argparse
import matplotlib.pyplot as plt
import logging
from itertools import groupby
from scipy.stats import linregress

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Functional Data Processing Pipeline with Enhanced Stats and Viz")
parser.add_argument("input_file", help="Path to input CSV or JSON file")
parser.add_argument("--group_by", default="region", help="Column to group by for aggregation")
parser.add_argument("--value", default="sales", help="Numeric value column to process")
parser.add_argument("--date", default="date", help="Date column to parse")
parser.add_argument("--threshold", type=float, default=700.0, help="Filter threshold for value > this")
parser.add_argument("--output", default="processed_data.csv", help="Output CSV file")
args = parser.parse_args()

# Function to load data as list
def load_data(file_path):
    ext = file_path.lower().split('.')[-1]
    if ext == 'csv':
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            return list(reader)
    elif ext == 'json':
        with open(file_path, 'r') as file:
            data = json.load(file)
            return list(data)
    else:
        raise ValueError("Unsupported file format. Use CSV or JSON.")

# Impute missing values 
def impute_missing(data, value_col, group_by_col, date_col):
    # Numerical: compute mean from non-missing
    num_values = [float(row[value_col]) for row in data if value_col in row and row[value_col] not in (None, '', '0')]
    num_mean = statistics.mean(num_values) if num_values else 0.0
    
    # Categorical: compute mode from non-missing
    cat_values = [row[group_by_col] for row in data if group_by_col in row and row[group_by_col] not in (None, '')]
    cat_mode = statistics.mode(cat_values) if cat_values else 'Unknown'
    
    # Date: for simplicity, use mode date if available
    date_values = [row[date_col] for row in data if date_col in row and row[date_col] not in (None, '')]
    date_mode = statistics.mode(date_values) if date_values else None
    
    # Impute
    for row in data:
        if value_col not in row or row[value_col] in (None, ''):
            row[value_col] = round(num_mean, 2)
        if group_by_col not in row or row[group_by_col] in (None, ''):
            row[group_by_col] = cat_mode
        if date_col not in row or row[date_col] in (None, '') and date_mode:
            row[date_col] = date_mode
    
    return data

# Function: Standardize numerical to float
def standardize_value(row, value_col):
    try:
        return {**row, value_col: round(float(row[value_col]), 2)}
    except ValueError:
        logging.warning(f"Invalid value after imputation: {row[value_col]}")
        return None

# Function: Standardize categorical (ensure str, strip whitespace)
def standardize_categorical(row, cat_col):
    val = row.get(cat_col, 'Unknown')
    return {**row, cat_col: str(val).strip()}

# Function: Parse date, return None if invalid
def parse_date(row, date_col):
    if date_col not in row or not row[date_col]:
        return None
    formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            parsed = datetime.strptime(row[date_col], fmt)
            return {**row, date_col: parsed.strftime('%Y-%m-%d')}
        except ValueError:
            pass
    logging.warning(f"Invalid date format for: {row[date_col]}")
    return None

# Function: Filter value > threshold
def filter_high_value(row, value_col, threshold):
    return row.get(value_col, 0) > threshold

# Function: Aggregate total value per group_by using reduce
def aggregate_values(data, group_by_col, value_col):
    def reducer(acc, row):
        key = row.get(group_by_col, 'Unknown')
        acc[key] = acc.get(key, 0) + row[value_col]
        return acc
    return reduce(reducer, data, {})

# Pipeline: Compose functions
def process_pipeline(input_data, args):
    growth_col = f"{args.value}_growth_pct"
    
    # Impute missing values first
    imputed = impute_missing(input_data, args.value, args.group_by, args.date)
    
    # Standardize value (filter None for invalid)
    standardized = list(filter(None, (standardize_value(row, args.value) for row in imputed)))
    
    # Standardize group_by
    standardized_group = [standardize_categorical(row, args.group_by) for row in standardized]
    
    # Parse dates (filter None for invalid dates)
    parsed = list(filter(None, (parse_date(row, args.date) for row in standardized_group)))
    
    # Filter high value
    filtered = [row for row in parsed if filter_high_value(row, args.value, args.threshold)]
    
    # Sort by group_by and date for sequential growth
    if args.date in filtered[0] if filtered else False:  # Check if date present
        sorted_data = sorted(filtered, key=lambda r: (r[args.group_by], datetime.strptime(r[args.date], '%Y-%m-%d')))
    else:
        sorted_data = filtered  # No date, no sort for growth
    
    # Compute sequential growth per group
    processed = []
    for key, group_iter in groupby(sorted_data, key=lambda r: r[args.group_by]):
        group = list(group_iter)
        for i, row in enumerate(group):
            if i == 0:
                growth = 0.0
            else:
                prev_val = group[i-1][args.value]
                growth = ((row[args.value] - prev_val) / prev_val * 100) if prev_val != 0 else 0.0
            processed.append({**row, growth_col: round(growth, 2)})
    
    return processed

# Statistical summaries, including trend analysis
def compute_stats(data, value_col, date_col):
    value_list = [row[value_col] for row in data]
    if not value_list:
        return {
            f"mean_{value_col}": 0,
            f"median_{value_col}": 0,
            f"variance_{value_col}": 0,
            f"stdev_{value_col}": 0,
            f"min_{value_col}": 0,
            f"max_{value_col}": 0,
            f"mode_{value_col}": "No data",
            "trend_slope": 0,
            "correlation_with_time": 0
        }
    stats = {
        f"mean_{value_col}": statistics.mean(value_list),
        f"median_{value_col}": statistics.median(value_list),
        f"variance_{value_col}": statistics.variance(value_list),
        f"stdev_{value_col}": statistics.stdev(value_list),
        f"min_{value_col}": min(value_list),
        f"max_{value_col}": max(value_list),
    }
    try:
        stats[f"mode_{value_col}"] = statistics.mode(value_list)
    except statistics.StatisticsError:
        stats[f"mode_{value_col}"] = "No unique mode"
    
    # Trend analysis: linear regression on time vs value
    if date_col and all(date_col in row for row in data):
        sorted_data = sorted(data, key=lambda r: r[date_col])
        time_nums = [datetime.strptime(r[date_col], '%Y-%m-%d').toordinal() for r in sorted_data]
        values = [r[value_col] for r in sorted_data]
        if len(value_list) > 1:
            result = linregress(time_nums, values)
            stats["trend_slope"] = round(result.slope, 4)
            stats["correlation_with_time"] = round(result.rvalue, 4)
        else:
            stats["trend_slope"] = 0
            stats["correlation_with_time"] = 0
    else:
        stats["trend_slope"] = 0
        stats["correlation_with_time"] = 0
    
    return stats

# Visualization functions (side effects isolated)
def visualize_aggregates(aggregates, group_by_col, value_col):
    if not aggregates:
        return
    keys = list(aggregates.keys())
    values = list(aggregates.values())
    plt.figure(figsize=(10, 6))
    plt.bar(keys, values)
    plt.xlabel(group_by_col.capitalize())
    plt.ylabel(f"Total {value_col.capitalize()}")
    plt.title(f"Aggregate {value_col.capitalize()} by {group_by_col.capitalize()}")
    plt.savefig('aggregates_bar.png')
    plt.close()

def visualize_histogram(value_list, value_col):
    if not value_list:
        return
    plt.figure(figsize=(10, 6))
    plt.hist(value_list, bins=10, edgecolor='black')
    plt.xlabel(value_col.capitalize())
    plt.ylabel('Frequency')
    plt.title(f"Histogram of {value_col.capitalize()} Values")
    plt.savefig('value_histogram.png')
    plt.close()

def visualize_trend(date_aggregates, date_col, value_col):
    if not date_aggregates:
        return
    keys = sorted(date_aggregates.keys())
    values = [date_aggregates[k] for k in keys]
    plt.figure(figsize=(10, 6))
    plt.plot(keys, values, marker='o')
    plt.xlabel(date_col.capitalize())
    plt.ylabel(f"Total {value_col.capitalize()}")
    plt.title(f"Trend of {value_col.capitalize()} Over Time")
    plt.xticks(rotation=45)
    plt.savefig('trend_line.png')
    plt.close()

# Main execution
input_data = load_data(args.input_file)
processed_data = process_pipeline(input_data, args)

if not processed_data:
    print("No data after processing.")
    sys.exit(0)

# Aggregates by group
group_aggregates = aggregate_values(processed_data, args.group_by, args.value)

# Aggregates by date for trend analysis
date_aggregates = aggregate_values(processed_data, args.date, args.value)

# Extract value list for stats and viz
value_list = [row[args.value] for row in processed_data]

# Stats with trend
stats = compute_stats(processed_data, args.value, args.date)

# Output to console (aggregates + stats)
print("Group Aggregates:", group_aggregates)
print("Date Aggregates (for Trend):", date_aggregates)
for key, val in stats.items():
    print(f"{key.capitalize()}: {val}")

# Generate visualizations
visualize_aggregates(group_aggregates, args.group_by, args.value)
visualize_histogram(value_list, args.value)
visualize_trend(date_aggregates, args.date, args.value)
print("Visualizations saved as 'aggregates_bar.png', 'value_histogram.png', and 'trend_line.png'")

# Save processed data to CSV
def save_csv(data, file_path):
    if not data:
        return
    fieldnames = list(data[0].keys())
    with open(file_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

save_csv(processed_data, args.output)