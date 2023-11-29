import _csv
import csv
import os

class PyCSV2:

    def __check_dir_exist(self, dirpath):
        """
        检验 save_dir 是否存在，如果不存在则创建该文件夹。
        :return: None
        """
        if not os.path.exists(dirpath):
            raise FileNotFoundError(f'{dirpath} 目录不存在，请检查！')
        if not os.path.isdir(dirpath):
            raise TypeError(f'{dirpath} 目标路径不是文件夹，请检查！')

    def __check_file_exist(self, csv_path):
        """
        检验 csv_path 是否是CSV文件。
        :return: None
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f'{csv_path} 文件不存在，请检查文件路径！')

        if not os.path.isfile(csv_path):
            raise TypeError(f'{csv_path} 路径非文件格式，请检查！')

        # 文件存在路径
        self.file_path_root = os.path.split(csv_path)[0]
        # 文件名称
        self.filename = os.path.split(csv_path)[1].replace('.csv', '').replace('.CSV', '')
        # 文件后缀
        self.extension = os.path.splitext(csv_path)[1]

        if self.extension.upper() != '.CSV':
            raise TypeError(f'{csv_path} 文件类型错误，非CSV文件类型，请检查！')

    def __check_name(self):
        """
        检查文件名称是否 .csv 结尾
        :return:
        """
        if not self.save_path.upper().endswith('.CSV'):
            raise TypeError('文件名称设置错误')

    def __check_singal_dir(self, file_list):
        """
        检查需要被合并的csv文件所在文件夹是否符合要求。
        1. 不应该存在除csv文件以外的文件
        2. 不应该存在文件夹。
        :return:
        """
        for file in file_list:
            if os.path.isdir(file):
                raise EnvironmentError(f'发现文件夹 {file}, 当前文件夹中存其他文件夹，请检查！')
            if not file.upper().endswith('.CSV'):
                raise EnvironmentError(f'发现非CSV文件：{file}, 请确保当前文件夹仅存放csv文件！')

    def split_csv(self, csv_path, save_dir, split_line=100000, csv_encoding='utf-8'):

        self.csv_path = csv_path
        self.save_dir = save_dir

        # 检测csv文件路径和保存路径是否符合规范
        self.__check_dir_exist(self.save_dir)
        self.__check_file_exist(self.csv_path)

        self.encoding = csv_encoding
        self.split_line = split_line

        self.file_size = round(os.path.getsize(self.csv_path) / 1024 / 1024, 2)

        self.line_numbers = 0

        i = 0
        with open(csv_path, 'r', encoding=csv_encoding, errors='ignore') as file:
            reader = csv.reader((line.replace('\0', '') for line in file))
            chunk = []
            for row in reader:
                try:
                    chunk.append(row)
                    if len(chunk) == self.split_line:
                        i += 1
                        self.line_numbers += len(chunk)

                        save_filename = os.path.join(self.save_dir, self.filename + "_" + str(i) + self.extension)
                        print(f"{save_filename} 已经生成！")

                        with open(save_filename, 'w', newline='', encoding='utf-8') as output_file:
                            writer = csv.writer(output_file)
                            writer.writerows(chunk)

                        chunk = []
                except _csv.Error as e:
                    print("error => ", e)
                    print("row => ", str(row))

            # 处理最后一个未满的数据块
            if chunk:
                i += 1
                self.line_numbers += len(chunk)

                save_filename = os.path.join(self.save_dir, self.filename + "_" + str(i) + self.extension)
                print(f"{save_filename} 已经生成！")

                with open(save_filename, 'w', newline='', encoding='utf-8') as output_file:
                    writer = csv.writer(output_file)
                    writer.writerows(chunk)

                print('数据全部切分完毕!')


if __name__ == '__main__':
    csv_path = r'F:\data\company\8亿+企业查数据\年报\年报.csv'
    save_dir = r'F:\data\company\8亿+企业查数据\年报\business'

    PyCSV2().split_csv(csv_path=csv_path, save_dir=save_dir, split_line=1000000)


    # csv_path1 = r'F:\data\company\8亿+企业查数据\商标信息\商标信息.csv'
    # save_dir1 = r'F:\data\company\8亿+企业查数据\商标信息\business'
    # PyCSV2().split_csv(csv_path=csv_path1, save_dir=save_dir1, split_line=1000000)
