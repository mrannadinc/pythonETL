#!/bin/env python3

from encodings import utf_8
import sys
import pandas as pd


class Transform:
    def __init__(self):
        print("Starting: ")

    def csv_to_excel(self):
        read_file = pd.read_csv(r'C:\Users\user\OneDrive\Desktop\webinar\EnokSatYaplanleler.csv')
        read_file.to_excel(r'C:\Users\user\OneDrive\Desktop\webinar\Müşteri Analizi.xlsx', index=None, header= True, encoding='utf-8')


def main():
    transform_data = Transform()
    #transform_data.csv_to_excel()

    print("Sdjkfsdkf TUT".capitalize())


if __name__ == '__main__':
    main()
    sys.exit()
