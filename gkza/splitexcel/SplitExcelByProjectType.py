import pandas as pd

str1 = " - "
str2 = ", "
str3 = ", & "
str4 = " & "
str5 = "/"
str6 = " "

def get_excel(value):

    if str1 in value:
        value = value.replace(str1, "_")

    if str2 in value:
        value = value.replace(str2, "_")

    if str3 in value:
        value = value.replace(str3, "_")

    if str4 in value:
        value = value.replace(str4, "_")

    if str5 in value:
        value = value.replace(str5, "-")

    if str6 in value:
        value = value.replace(str6, "_")

    return value

if __name__ == '__main__':
    file_path = "E:\hadoop\data\ExcelOut\project-01.xlsx"

    df = pd.read_excel(file_path)

    unique_values = df['项目类别'].unique()

    for value in unique_values:

        subset = df[df['项目类别'] == value]

        file_name = get_excel(value)
        output_path = f'E:\hadoop\data\ExcelOut\{file_name}.xlsx'

        subset.to_excel(output_path, index=False)