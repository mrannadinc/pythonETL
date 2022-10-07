#!/bin/env python3
import ftplib
import sys
import os
import time
from datetime import timedelta, datetime
from ftplib import FTP

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, exc

DATE = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


class Transform:
    def __init__(self):
        print("Starting: " + "SALES" + DATE + ".cvs")
        self.mysql_connection = self.mysql_connection()
        self.ftp = FTP('********')

    def mysql_connection(self):
        try:
            engine = create_engine("mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('****',
                                                                                             '******',
                                                                                             '******',
                                                                                             3307,
                                                                                             '*******'))
            return engine
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))

    def ftp_start_connection(self):
        try:
            self.ftp.login('************', '*************')
        except ftplib.all_errors:
            print(str(ftplib.all_errors))

    def ftp_close_connection(self):
        try:
            self.ftp.close()
        except ftplib.all_errors:
            print(str(ftplib.all_errors))

    def download_csv_to_server(self):
        start = datetime.now()
        try:
            files = self.ftp.nlst()
            file_name = "SALES" + DATE + ".cvs"
            if file_name in files:
                print("Downloading... " + file_name)
                self.ftp.retrbinary("RETR " + file_name,
                                    open("path" + file_name, 'wb').write)
            else:
                print("file is not found")
                self.ftp_close_connection()
                exit()

            # Until they fix the extension
            os.rename("path_to_csv" + DATE + ".cvs",
                      "path_to_csv" + DATE + ".csv")

            end = datetime.now()
            diff = end - start
            print('File downloaded for ' + str(diff.seconds) + 's')
        except ftplib.all_errors:
            print(str(ftplib.all_errors))
            self.ftp_close_connection()

    def truncate_temp_data(self):
        try:
            self.mysql_connection.execute("truncate table_name;")
            print("Dardanel table_name table is truncated!")
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def alter_dardanel_date_temp(self):
        try:
            self.mysql_connection.execute("ALTER TABLE table_name MODIFY TRX_DATE varchar(20);")
            self.mysql_connection.execute("ALTER TABLE table_name MODIFY OLUSTURMATARIHI varchar(20);")
            print("Dardanel table_name table TRX_DATE OLUSTURMATARIHI columns altered as varchar!")
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def write_mysql_data_temp(self):
        print("Data inserting...")
        file_name = "path_to_csv" + DATE + ".csv"
        batch_no = 0
        try:
            for chunk in pd.read_csv(file_name, encoding='utf8', sep=';', error_bad_lines=False, chunksize=10000):
                chunk["date_imported"] = pd.to_datetime('today')
                chunk.to_sql('table_name', con=self.mysql_connection, if_exists='append', index=False, index_label='id',
                             dtype={'TRX_DATE': sqlalchemy.types.VARCHAR(length=20),
                                    'OLUSTURMATARIHI': sqlalchemy.types.VARCHAR(length=20),
                                    'TRX_NUMBER': sqlalchemy.types.VARCHAR(length=20),
                                    'BELGEKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'DISTGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'DISTKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'DISTAD': sqlalchemy.types.VARCHAR(length=100),
                                    'STKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'STAD': sqlalchemy.types.VARCHAR(length=100),
                                    'SITE_USE_ID': sqlalchemy.types.VARCHAR(length=20),
                                    'MARKETKODU': sqlalchemy.types.VARCHAR(length=20),
                                    'MUSTERI': sqlalchemy.types.VARCHAR(length=200),
                                    'YERLESIM': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERISITEKANALSEVKIYAT': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERIGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERIEKGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERI_SORUMLUSU': sqlalchemy.types.VARCHAR(length=100),
                                    'KALEM_KODU': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIR_TIPI': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIS_ANA_GRUP': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIS_ALT_GRUP': sqlalchemy.types.VARCHAR(length=50),
                                    'MARKA': sqlalchemy.types.VARCHAR(length=100),
                                    'UOM_CODE': sqlalchemy.types.VARCHAR(length=10),
                                    'FATURA_MIKTARI': sqlalchemy.types.Integer(),
                                    'BIRIM_FIYAT': sqlalchemy.types.Numeric(),
                                    'BRUT_SATIS': sqlalchemy.types.Numeric(),
                                    'SATIS_INDIRIMI_TL': sqlalchemy.types.Integer(),
                                    'VERGI': sqlalchemy.types.Numeric(),
                                    'NET_SATIS': sqlalchemy.types.Numeric(),
                                    'FATURA_TIPI': sqlalchemy.types.VARCHAR(length=50),
                                    'KRITER': sqlalchemy.types.VARCHAR(length=50),
                                    'URETICI': sqlalchemy.types.VARCHAR(length=100),
                                    'PANORAMA_FATURANO': sqlalchemy.types.VARCHAR(length=20),
                                    'PANORAMA_IRSALIYENO': sqlalchemy.types.VARCHAR(length=20),
                                    'SIRKET': sqlalchemy.types.VARCHAR(length=100)})
                batch_no += 1
                print('index: {}'.format(batch_no))
                self.mysql_connection.execute("COMMIT;")
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def update_dardanel_date_trx_temp(self):
        try:
            total_start_time = time.time()
            self.mysql_connection.execute(
                f"UPDATE table_name SET "
                f"TRX_DATE = str_to_date(TRX_DATE, '%%d-%%M-%%Y') WHERE date(date_imported) = date(SYSDATE())")
            self.mysql_connection.execute("ALTER TABLE table_name MODIFY TRX_DATE date")
            total_end_time = time.time()
            print("Dardanel table_name table TRX_DATE updated!")
            print("Update completed with(time) : ", total_end_time - total_start_time)
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def update_dardanel_date_ot_temp(self):
        try:
            total_start_time = time.time()
            self.mysql_connection.execute(
                f"UPDATE table_name SET "
                f"OLUSTURMATARIHI = str_to_date(OLUSTURMATARIHI, '%%d-%%M-%%Y') WHERE date(date_imported) = date(SYSDATE())")
            self.mysql_connection.execute("ALTER TABLE table_name MODIFY OLUSTURMATARIHI date")
            total_end_time = time.time()
            print("Dardanel table_name table OLUSTURMATARIHI updated!")
            print("Update completed with(time) : ", total_end_time - total_start_time)
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def get_temp_min_date(self):
        query = 'SELECT min(TRX_DATE) from table_name'
        try:
            df = pd.read_sql_query(query, self.mysql_connection)
            if df.size == 0:
                print("Download file is empty!")
                df_value = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
                return df_value
            else:
                df_value = df['min(TRX_DATE)'].values[0]
                return df_value
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def delete_data(self, after_day):
        print(after_day)
        try:
            self.mysql_connection.execute(f"delete from table_name "
                                          f"where TRX_DATE >= '{after_day}'")
            print(f"Dardanel table_name table {after_day} after days deleted!")
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def write_mysql_data(self):
        print("Data inserting...")
        query = "select * from table_name;"
        batch_no = 0
        try:
            for chunk in pd.read_sql_query(query, self.mysql_connection, chunksize=10000):
                chunk["date_imported"] = pd.to_datetime('today')
                chunk.to_sql('table_name', con=self.mysql_connection, if_exists='append', index=False, index_label='id',
                             dtype={'TRX_DATE': sqlalchemy.types.DATE(),
                                    'OLUSTURMATARIHI': sqlalchemy.types.DATE(),
                                    'TRX_NUMBER': sqlalchemy.types.VARCHAR(length=20),
                                    'BELGEKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'DISTGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'DISTKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'DISTAD': sqlalchemy.types.VARCHAR(length=100),
                                    'STKOD': sqlalchemy.types.VARCHAR(length=20),
                                    'STAD': sqlalchemy.types.VARCHAR(length=100),
                                    'SITE_USE_ID': sqlalchemy.types.VARCHAR(length=20),
                                    'MARKETKODU': sqlalchemy.types.VARCHAR(length=20),
                                    'MUSTERI': sqlalchemy.types.VARCHAR(length=200),
                                    'YERLESIM': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERISITEKANALSEVKIYAT': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERIGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERIEKGRUP': sqlalchemy.types.VARCHAR(length=100),
                                    'MUSTERI_SORUMLUSU': sqlalchemy.types.VARCHAR(length=100),
                                    'KALEM_KODU': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIR_TIPI': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIS_ANA_GRUP': sqlalchemy.types.VARCHAR(length=50),
                                    'SATIS_ALT_GRUP': sqlalchemy.types.VARCHAR(length=50),
                                    'MARKA': sqlalchemy.types.VARCHAR(length=100),
                                    'UOM_CODE': sqlalchemy.types.VARCHAR(length=10),
                                    'FATURA_MIKTARI': sqlalchemy.types.Integer(),
                                    'BIRIM_FIYAT': sqlalchemy.types.Numeric(),
                                    'BRUT_SATIS': sqlalchemy.types.Numeric(),
                                    'SATIS_INDIRIMI_TL': sqlalchemy.types.Integer(),
                                    'VERGI': sqlalchemy.types.Numeric(),
                                    'NET_SATIS': sqlalchemy.types.Numeric(),
                                    'FATURA_TIPI': sqlalchemy.types.VARCHAR(length=50),
                                    'KRITER': sqlalchemy.types.VARCHAR(length=50),
                                    'URETICI': sqlalchemy.types.VARCHAR(length=100),
                                    'PANORAMA_FATURANO': sqlalchemy.types.VARCHAR(length=20),
                                    'PANORAMA_IRSALIYENO': sqlalchemy.types.VARCHAR(length=20),
                                    'SIRKET': sqlalchemy.types.VARCHAR(length=100)})
                batch_no += 1
                print('index: {}'.format(batch_no))
                self.mysql_connection.execute("COMMIT;")
        except exc.SQLAlchemyError:
            print(str(exc.SQLAlchemyError))
            self.mysql_connection.execute("ROLLBACK;")

    def upload_csv_to_imported(self):
        start = datetime.now()
        file_name = "SALES" + DATE + ".csv"

        try:
            with open("path" + file_name, "rb") as file:
                self.ftp.storbinary(f'STOR //imported//{file_name}', file)

            end = datetime.now()
            diff = end - start
            print('File uploaded to imported ' + str(diff.seconds) + 's')
            self.ftp.delete("SALES" + DATE + ".cvs")
            print('Remote file deleted')
        except ftplib.all_errors:
            print(str(ftplib.all_errors))
            self.ftp_close_connection()


def main():
    transform_data = Transform()
    total_start_time = time.time()
    ftp_start_time = time.time()
    transform_data.ftp_start_connection()
    transform_data.download_csv_to_server()
    ftp_end_time = time.time()
    print("Download CSV completed with(time) : ", ftp_end_time - ftp_start_time)

    print("****************************************************")

    # temp table insert
    print("Data inserting to temp table...")
    temp_start_time = time.time()
    transform_data.truncate_temp_data()
    transform_data.alter_dardanel_date_temp()
    temp_write_start_time = time.time()
    transform_data.write_mysql_data_temp()
    temp_write_end_time = time.time()
    print("Database temp insert completed with(time) : ", temp_write_end_time - temp_write_start_time)
    transform_data.update_dardanel_date_trx_temp()
    transform_data.update_dardanel_date_ot_temp()
    temp_end_time = time.time()
    print("Temp process completed with(time) : ", temp_end_time - temp_start_time)

    print("****************************************************")

    # main table insert
    print("Data inserting to main table...")
    main_start_time = time.time()
    after_day = transform_data.get_temp_min_date()
    transform_data.delete_data(after_day)
    write_start_time = time.time()
    transform_data.write_mysql_data()
    write_end_time = time.time()
    print("Database insert completed with(time) : ", write_end_time - write_start_time)
    main_end_time = time.time()
    print("Main process completed with(time) : ", main_end_time - main_start_time)

    print("****************************************************")

    upload_start_time = time.time()
    transform_data.upload_csv_to_imported()
    upload_end_time = time.time()
    print("Upload CSV completed with(time) : ", upload_end_time - upload_start_time)
    transform_data.ftp_close_connection()

    total_end_time = time.time()
    print("Transform completed with(time) : ", total_end_time - total_start_time)


if __name__ == '__main__':
    main()
    sys.exit()
