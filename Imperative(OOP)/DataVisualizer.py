import matplotlib.pyplot as plt
import seaborn as sns

class DataVisualizer:
    def __init__(self , transformer):
        sns.set(style="whitegrid") 
        self.transformer = transformer

    def bar_chart(self, data, value_col, category_col):
        """
        Creates a bar chart of total value per category.
        """
        # Aggregate sums per category
        aggregates = {}
        for row in data:
            cat = row.get(category_col, "Unknown")
            val = row.get(value_col)
            if val is None:
                continue
            aggregates[cat] = aggregates.get(cat, 0) + val

        if not aggregates:
            print("No data to plot.")
            return None

        fig, ax = plt.subplots(figsize=(10,6))
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
        
    def correlation_heatmap(self, data, numeric_cols, analyzer):
        """
        Plots a correlation heatmap using DataAnalyzer's correlation_matrix.
        """
        matrix = analyzer.correlation_matrix(data, numeric_cols)

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
