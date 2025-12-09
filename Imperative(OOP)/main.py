from DataStandardizer import DataStandardizer
from DataTransformer import DataTransformer
from MissingDataHandler import MissingDataHandler
from DataValidator import DataValidator
from DataAnalyzer import DataAnalyzer
from DataVisualizer import DataVisualizer
from FileLoader import FileLoader
import matplotlib.pyplot as plt

# ---------------------------------------------------
data_loader = FileLoader("data/mine.csv", "Imperative(OOP)/output_data.csv")
data = data_loader.load()
print("\n===== RAW DATA =====")
for row in data:
    print(row)
# ---------------------------------------------------
validator = DataValidator()
data = validator.remove_none_keys(data)
# ---------------------------------------------------
missing = MissingDataHandler()

missing_info, missing_stats = missing.detect_missing(data)
print("\nMissing Info:", missing_info)
print("Missing Stats:", missing_stats)

# Fill missing sales with mean
data = missing.impute_mean(data, "sales")

print("\n===== AFTER IMPUTATION =====")
for row in data:
    print(row)

# ---------------------------------------------------
std = DataStandardizer()

# convert numeric columns
data = std.standardize_numeric_column(data, "sales")

# unify categorical (trim spaces, convert to str)
data = std.standardize_categorical_column(data, "region")

# normalize dates into YYYY-MM-DD
data = std.standardize_date_column(data, "date")

print("\n===== AFTER STANDARDIZATION =====")
for row in data:
    print(row)



# ---------------------------------------------------

transformer = DataTransformer()

filtered_data = transformer.filter_rows(data, "sales", threshold=1000)

print("\n===== FILTER (sales > 1000) =====")
for row in filtered_data:
    print(row)

aggregated = transformer.aggregate(data, "region", "sales")

print("\n===== AGGREGATED SALES BY REGION =====")
print(aggregated)

# ---------------------------------------------------
analyzer = DataAnalyzer()

summary_sales = analyzer.summary(data, "sales")
print("\n===== SALES SUMMARY =====")
print(summary_sales)

corr = analyzer.correlation(data, "sales", "sales")   # trivial example
print("\nCorrelation(sales, sales):", corr)

# ---------------------------------------------------

visualizer = DataVisualizer(transformer , analyzer)

print("\nShowing Charts...")

# Bar chart
fig1 = visualizer.bar_chart(data, "sales", "region")
if fig1: fig1.show()
plt.show()  

# Line chart
fig2 = visualizer.line_chart(data, "region", "sales")
if fig2: fig2.show()
plt.show() 

# Correlation heatmap
fig3 = visualizer.correlation_heatmap(
    data, 
    numeric_cols=["sales"],
)
if fig3: fig3.show()
plt.show()  

print("\n===== END OF PIPELINE EXECUTION =====")
