import pandas as pd
import glob
import base64

def de_base64(str):
    if pd.notnull(str) and len(str) > 1:
        decode_str = base64.b64decode(str)
        decode_data = decode_str.decode('utf-8')
        return decode_data

if __name__ == '__main__':

    file_path = "E:\\data\\talent\\*"
    out_path = "E:\data\credential1.json"

    file_list = glob.glob(file_path)

    dfs = []

    for file in file_list:
        df = pd.read_json(file, lines=True)
        dfs.append(df)

    combined_df = pd.concat(dfs)

    duplicate_uuids = {}
    tag = 0
    one_tag = 0
    for row_index, row in combined_df.iterrows():
        source = row['source']
        uuid = row['uuid']
        personal = row['personal_profile']

        source = de_base64(source)
        personal = de_base64(personal)

        if "中国工程院" in source or "中国工程院" in personal:
            if uuid in duplicate_uuids:
                value = duplicate_uuids.get(uuid)
                duplicate_uuids[uuid] = value + 1
            else:
                duplicate_uuids[uuid] = 1

            tag += 1

    print("===>", tag)


    df1 = pd.read_excel("E:\\hadoop\\data\\ExcelOut\\talent_collect.xlsx")
    hive_uuids = []
    for row_index, row in df1.iterrows():
        uuid1 = row['uuid']
        hive_uuids.append(uuid1)


    for key in duplicate_uuids.keys():
        value = duplicate_uuids[key]
        if value == 1:
            if key not in hive_uuids:
                print("uuid => ", key)