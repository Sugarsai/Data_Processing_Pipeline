import matplotlib.pyplot as plt
import seaborn as sns

class DataVisualizer:
    def __init__(self , transformer, analyzer):
        sns.set(style="whitegrid") 
        self.transformer = transformer
        self.analyzer = analyzer

    def bar_chart(self, data, value_col, category_col):
        aggregates = self.transformer.aggregate(data, category_col, value_col)

        if not aggregates:
            print("No data to plot.")
            return None

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(aggregates.keys(), aggregates.values(), color='skyblue')
        ax.set_xlabel(category_col.capitalize())
        ax.set_ylabel(f"Total {value_col.capitalize()}")
        ax.set_title(f"Total {value_col.capitalize()} by {category_col.capitalize()}")
        plt.xticks(rotation=45)
        plt.tight_layout()
        return fig

    def line_chart(self, data, group_col, value_col):
        aggregated = self.transformer.aggregate(data, group_col, value_col)

        if not aggregated:
            print("No data to plot after aggregation.")
            return None

        # Prepare x and y lists
        xs = list(aggregated.keys())
        ys = list(aggregated.values())

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(xs, ys, marker='o', linestyle='-', color='orange')
        ax.set_xlabel(group_col.capitalize())
        ax.set_ylabel(value_col.capitalize())
        ax.set_title(f"{value_col.capitalize()} by {group_col.capitalize()}")
        plt.xticks(rotation=45)
        plt.tight_layout()
        return fig
        
    def correlation_heatmap(self, data, numeric_cols):
        
        matrix = self.analyzer.correlation_matrix(data, numeric_cols)

        if not matrix:
            print("No data to plot correlation.")
            return None

        # Convert matrix to 2D list for seaborn
        corr_values = [[matrix[r][c] if matrix[r][c] is not None else 0 for c in numeric_cols] for r in numeric_cols]

        fig, ax = plt.subplots(figsize=(8,6))
        sns.heatmap(corr_values, annot=True, fmt=".2f", xticklabels=numeric_cols, yticklabels=numeric_cols, cmap="coolwarm", cbar=True)
        ax.set_title("Correlation Heatmap")
        plt.tight_layout()
        return fig
