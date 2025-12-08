from FileLoader import FileLoader
from DataValidator import DataValidator
from MissingDataHandler import MissingDataHandler
from DataStandardizer import DataStandardizer
from DataAnalyzer import DataAnalyzer
from DataVisualizer import DataVisualizer
from DataTransformer import DataTransformer
import matplotlib.pyplot as plt


fl = FileLoader('data/sales_data.csv', 'Imperative(OOP)/dataOOP/output.json')
dv = DataValidator()
mdh = MissingDataHandler()
ds = DataStandardizer()
da = DataAnalyzer()
dt = DataTransformer()
dvz = DataVisualizer(dt)

data = fl.load()
#for r in data:
   # print(r)
data = dv.remove_none_keys(data)
mi,ms = mdh.detect_missing(data)
#print("Missing Info:", mi)
#print("Missing Stats:", ms)
#data = mdh.fill_default(data, 'sales',"666")
data = ds.standardize_numeric_column(data, 'sales')
#data = ds.standardize_categorical_column(data, 'region')
#data = ds.standardize_date_column(data, 'date')
#summary = da.summary(data, 'sales')
#print("Summary of sales:", summary)

#correlation = da.correlation(data, 'sales', 'sales')
#print("Correlation between sales and profit:", correlation)

#matrix = da.correlation_matrix(data, ['sales', 'sales', 'sales'])
#print("Correlation matrix:", matrix)

#trend = da.trend(data, 'sales', 'sales')
#print("Trend analysis between sales and profit:", trend)

#fig1 = dvz.bar_chart(data, 'sales', 'region')
#if fig1:
    #fig1.savefig('aggregates_bar.png')
    #plt.close(fig1)

fig2 = dvz.line_chart(data, 'date', 'sales')
if fig2:
    fig2.savefig('trend_line.png')
    plt.close(fig2)

#fig3 = dvz.correlation_heatmap(data, ['sales', 'sales', 'sales'], da)
#if fig3:
#    fig3.savefig('correlation_heatmap.png')
#    plt.close(fig3)

data = dt.filter_rows(data, 'sales', 1000)
aggregates = dt.aggregate(data, 'region', 'sales')
for r in data:
    print(r)
print("Aggregates:", aggregates)
#fl.save(data)
#print("Data loaded and saved successfully.")