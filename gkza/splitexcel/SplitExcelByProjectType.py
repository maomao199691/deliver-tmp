import pandas as pd

if __name__ == '__main__':
    file_path = "E:\hadoop\data\ExcelOut\project-01.xlsx"

    df = pd.read_excel(file_path)

    unique_values = df['项目类别'].unique()

    for value in unique_values:

        subset = df[df['项目类别'] == value]

        output_path = f'E:\hadoop\data\ExcelOut\project_{value}.xlsx'

        subset.to_excel(output_path, index=False)