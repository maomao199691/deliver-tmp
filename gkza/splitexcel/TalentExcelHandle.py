import pandas as pd


acad = '院士'

sub1 = '当选为'
sub2 = '当选'
sub4 = '聘为'
sub3 = '中国科学院'
sub5 = '中科院院士'


china = '中国'
sub6 = "外籍院士"
# 外籍院士
def getOther(str):
    if acad in str:
        if sub1 in str:
            index = str.rindex(sub1)
            str = str[index+len(sub1):]
            index1 = str.find(acad)
            str = str[:index1+len(acad)]
            if china in str:
                return str
            else:
                return sub6

        if sub2 in str:
            index = str.rindex(sub2)
            str = str[index+len(sub2):]
            index1 = str.find(acad)
            str = str[:index1+len(acad)]
            if china in str:
                return str
            else:
                return sub6

        if sub4 in str:
            index = str.rindex(sub4)
            str = str[index+len(sub4):]
            index1 = str.find(acad)
            str = str[:index1+len(acad)]
            if china in str:
                return str
            else:
                return sub6

if __name__ == '__main__':

    file_path = "E:\data\人才all1.xlsx"

    df = pd.read_excel(file_path, sheet_name='Sheet1')

    for row_index, row in df.iterrows():

        introduction = row['个人简介']
        title = row['职称']

        result = ''
        if pd.isnull(title):
            if pd.notnull(introduction) and acad in introduction:
                if  sub2 in introduction:
                    if sub3 in introduction:
                        if sub1 in introduction:
                            index = introduction.rindex(sub1)
                            introduction = introduction[index+len(sub1):]
                            index1 = introduction.find(acad)
                            if "院士)" in introduction or "院士）" in introduction:
                                new_title = introduction[:index1 + len(acad) + 1].rstrip()
                            if '中国科学院院士' in introduction:
                                new_title = introduction[:index1 + len(acad)].rstrip()
                            if "。" in new_title:
                                index2 = new_title.find("。")
                                new_title = new_title[:index2]
                            result = new_title
                            df.loc[row_index, '职称'] = result

                        else:
                            index = introduction.rindex(sub2)
                            introduction = introduction[index+len(sub2):]
                            index1 = introduction.find(acad)
                            if "院士)" in introduction or "院士）" in introduction:
                                new_title = introduction[:index1 + len(acad) + 1].rstrip()
                            if '中国科学院院士' in introduction:
                                new_title = introduction[:index1 + len(acad)].rstrip()
                            if "。" in new_title:
                                index2 = new_title.find("。")
                                new_title = new_title[:index2]
                            result = new_title
                            df.loc[row_index, '职称'] = result

                    elif sub5 in introduction:
                        if sub1 in introduction:
                            index = introduction.find(sub1)
                            introduction = introduction[index + len(sub1):]
                            index1 = introduction.find(acad)
                            new_title = introduction[:index1 + len(acad) + 1].rstrip()
                            if "。" in new_title:
                                index2 = new_title.find("。")
                                new_title = new_title[:index2]
                            result = new_title
                            df.loc[row_index, '职称'] = result
                        else:
                            index = introduction.find(sub2)
                            introduction = introduction[index + len(sub2):]
                            index1 = introduction.find(acad)
                            new_title = introduction[:index1 + len(acad) + 1].rstrip()
                            if "。" in new_title:
                                index2 = new_title.find("。")
                                new_title = new_title[:index2]
                            result = new_title
                            df.loc[row_index, '职称'] = result

                    else:
                        result = getOther(introduction)
                        df.loc[row_index, '职称'] = result


                if  sub4 in introduction:
                    if sub3 in introduction:
                        index = introduction.find(sub4)
                        introduction = introduction[index + len(sub4):]
                        index1 = introduction.find(acad)
                        if "院士)" in introduction or "院士）" in introduction:
                            new_title = introduction[:index1 + len(acad) + 1].rstrip()
                        if '中国科学院院士' in introduction:
                            new_title = introduction[:index1 + len(acad)].rstrip()
                        if "。" in new_title:
                            index2 = new_title.find("。")
                            new_title = new_title[:index2]
                        result = new_title
                        df.loc[row_index, '职称'] = result

                    elif sub5 in introduction:
                        if sub1 in introduction:
                            index = introduction.find(sub4)
                            introduction = introduction[index + len(sub4):]
                            index1 = introduction.find(acad)
                            new_title = introduction[:index1 + len(acad) + 1].rstrip()
                            if "。" in new_title:
                                index2 = new_title.find("。")
                                new_title = new_title[:index2]
                            result = new_title
                            df.loc[row_index, '职称'] = result

                    else:
                        result = getOther(introduction)
                        df.loc[row_index, '职称'] = result

    df.to_excel(file_path, index=False, sheet_name='Sheet1')