import pandas as pd


def get_domain(url):
    if url:
        if "//" in url:
            split = url.split("//")
            s = None
            try:
                s = split[1]
            except Exception as e:
                print("===>", s)
                return None

            if s.startswith("//"):
                s = s[2:]

            if "/" in s:
                i = s.index("/")
                sub_str = s[:i]
                if ":" in sub_str:
                    index = sub_str.index(":")
                    if index > 0:
                        sub_str = sub_str[:index]

                return sub_str

            i1 = s.find(":")
            if i1 > 0:
                s = s[:i1]

            return s

        if "/" in url:
            i = url.index("/")
            domain = url[:i]
            i1 = domain.index(":")
            if i1 > 0:
                domain = domain[:i1]

            return domain

    return url

if __name__ == '__main__':

    file_path = 'E:\data\锐寻数据总结.xlsx'

    df = pd.read_excel(file_path, sheet_name=10, header=0)

    filter_df = df.query('`是否在持续采` == "持续采集" and `数据采集渠道` == "爬虫采集"')

    collect_url = filter_df['数据源']

    tag = 0

    unique_domains = set()
    for url in collect_url:
        tag += 1
        # domain = get_domain(url)

        unique_domains.add(url)

    domain_str = '('
    domain_str += ','.join(f'"{d}"' for d in unique_domains) + ')'
    print(domain_str)
    print("size => ", len(unique_domains))


