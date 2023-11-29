import pandas as pd
import re

if __name__ == '__main__':
    excel_path = "E:\hadoop\data\ExcelOut\special_database_info_collect.xlsx"

    df = pd.read_excel(excel_path)
    df['full_text'] = df['full_text'].str.replace(r'@\[.*?\]@', '', regex=True)

    df.to_excel("E:\hadoop\data\ExcelOut\info_collect_02.xlsx", index=False)

