import sys
import time

import sqlalchemy
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import pymysql


class Transform:
    def __init__(self):
        print("Starting: ")
        start_time = time.time()
        self.windesk_start_mysql_server = self.windesk_start_mysql_server()
        self.turboard_start_mysql_server = self.turboard_start_mysql_server()
        self.windesk_mysql_connection = self.windesk_mysql_connection()
        self.turboard_mysql_engine_connection = self.turboard_mysql_engine_connection()
        second_time = time.time()
        print("--- %s seconds connection time --- " % (second_time - start_time))

    def windesk_start_mysql_server(self):
        mysql_server = SSHTunnelForwarder(
            '************',
            ssh_username="*********",
            ssh_password="***********",
            remote_bind_address=("************", 3306)
        )

        mysql_server.start()

        return mysql_server

    def turboard_start_mysql_server(self):
        mysql_server = SSHTunnelForwarder(
            '***********',
            ssh_username="*********",
            ssh_password="**********",
            remote_bind_address=("*************", 3307)
        )

        mysql_server.start()

        return mysql_server

    def windesk_mysql_connection(self):
        conn_info = {'host': '************',
                     'port': self.windesk_start_mysql_server.local_bind_port,
                     'user': "***********",
                     'password': "**********",
                     'database': "**************"}
        connection = pymysql.connect(**conn_info)
        return connection

    def turboard_mysql_engine_connection(self):
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}:{4}/{3}?charset=utf8'.format('************',
                                                                                         '**********',
                                                                                         '**********',
                                                                                         '**************',
                                                                                         self.turboard_start_mysql_server.local_bind_port))
        return engine

    def truncate_turboard_table(self):
        total_start_time = time.time()
        self.turboard_mysql_engine_connection.execute("truncate table ISSUEREPORT")
        print("Turboard ISSUEREPORT table truncated!")
        self.turboard_mysql_engine_connection.execute("truncate table CALLREPORT")
        print("Turboard CALLREPORT table truncated!")
        self.turboard_mysql_engine_connection.execute("truncate table PERSONALREPORT")
        print("Turboard PERSONALREPORT table truncated!")
        total_end_time = time.time()
        print("Truncate completed with(time) : ", total_end_time - total_start_time)

    def write_issuereport_data(self):
        batch_no = 0
        chunk_size = 1000
        for chunk in pd.read_sql_query("select CODE,"
                                       "SERVICENAME,"
                                       "IDATE,"
                                       "BPLANNED,"
                                       "CALLTYPE,"
                                       "BINA,"
                                       "KAT,"
                                       "KANAT,"
                                       "ZONE,"
                                       "STATUSPARENTCODE,"
                                       "BMSTATUS,"
                                       "CALLERNAME,"
                                       "COMPANY,"
                                       "TRIYAJ_REASON,"
                                       "SLANAME,"
                                       "DUZELTME_ZAMANI,"
                                       "HATA_PUANI as HATA_PUANI,"
                                       "replace(KESINTI_ORANI,',','.') as KESINTI_ORANI from issuereport;",
                                       self.windesk_mysql_connection,
                                       chunksize=10000):
            chunk.to_sql('ISSUEREPORT', con=self.turboard_mysql_engine_connection, if_exists='append', index=False,
                         index_label='id',
                         dtype={'CODE': sqlalchemy.types.VARCHAR(length=50),
                                'SERVICENAME': sqlalchemy.types.VARCHAR(length=255),
                                'IDATE': sqlalchemy.types.DATETIME(),
                                'CALLTYPE': sqlalchemy.types.VARCHAR(length=150),
                                'BINA': sqlalchemy.types.VARCHAR(length=300),
                                'KAT': sqlalchemy.types.VARCHAR(length=300),
                                'KANAT': sqlalchemy.types.VARCHAR(length=300),
                                'ZONE': sqlalchemy.types.VARCHAR(length=150),
                                'STATUSPARENTCODE': sqlalchemy.types.VARCHAR(length=150),
                                'BMSTATUS': sqlalchemy.types.VARCHAR(length=50),
                                'CALLERNAME': sqlalchemy.types.VARCHAR(length=50),
                                'COMPANY': sqlalchemy.types.VARCHAR(length=100),
                                'TRIYAJ_REASON': sqlalchemy.types.VARCHAR(length=5000),
                                'PERFORMANCE': sqlalchemy.types.VARCHAR(length=100),
                                'SLANAME': sqlalchemy.types.VARCHAR(length=255),
                                'DUZELTME_ZAMANI': sqlalchemy.types.DATETIME(),
                                'HATA_PUANI': sqlalchemy.types.VARCHAR(length=10),
                                'KESINTI_ORANI': sqlalchemy.types.VARCHAR(length=20)
                                })
            batch_no += 1
            print('index: {}'.format(batch_no))

    def write_callreport_data(self):
        batch_no = 0
        chunk_size = 1000
        for chunk in pd.read_sql_query("select * from callreport;",
                                       self.windesk_mysql_connection,
                                       chunksize=10000):
            chunk.to_sql('CALLREPORT', con=self.turboard_mysql_engine_connection, if_exists='append', index=False,
                         index_label='id',
                         dtype={'CALL_ID': sqlalchemy.types.Integer(),
                                'CALL_CODE': sqlalchemy.types.VARCHAR(length=20),
                                'CALL_CALLER': sqlalchemy.types.VARCHAR(length=50),
                                'CALL_DNIS': sqlalchemy.types.VARCHAR(length=500),
                                'CALL_CALLID': sqlalchemy.types.VARCHAR(length=200),
                                'CALL_IDATE': sqlalchemy.types.DATETIME(),
                                'CALL_RESULT': sqlalchemy.types.VARCHAR(length=150),
                                'CALL_STATUS': sqlalchemy.types.VARCHAR(length=150),
                                'CALL_DESCRIPTION': sqlalchemy.types.VARCHAR(length=500),
                                'COUNT_ISSUE': sqlalchemy.types.Integer(),
                                'CALL_SUMDESC2': sqlalchemy.types.VARCHAR(length=50),
                                'CALL_SUMDESC2_NAME': sqlalchemy.types.VARCHAR(length=5000),
                                'CALL_CATEGORY1': sqlalchemy.types.VARCHAR(length=100),
                                'CALL_CATEGORY1_NAME': sqlalchemy.types.VARCHAR(length=300),
                                'CALL_CATEGORY2': sqlalchemy.types.VARCHAR(length=100),
                                'CALL_CATEGORY2_NAME': sqlalchemy.types.VARCHAR(length=300),
                                'CALL_CATEGORY3': sqlalchemy.types.VARCHAR(length=100),
                                'CALL_CATEGORY3_NAME': sqlalchemy.types.VARCHAR(length=300),
                                'CALL_CATEGORY4': sqlalchemy.types.VARCHAR(length=100),
                                'CALL_CATEGORY4_NAME': sqlalchemy.types.VARCHAR(length=300),
                                'CALL_CATEGORY5': sqlalchemy.types.VARCHAR(length=100),
                                'CALL_CATEGORY5_NAME': sqlalchemy.types.VARCHAR(length=300)
                                })
            batch_no += 1
            print('index: {}'.format(batch_no))

    def write_personalreport_data(self):
        batch_no = 0
        chunk_size = 1000
        for chunk in pd.read_sql_query("SELECT ID,"
                                       "HR_CODE,"
                                       "REGISTRATIONNO,"
                                       "TRIDNO,"
                                       "NAME,"
                                       "SURNAME,"
                                       "SERVICE,"
                                       "COMPANY,"
                                       "STR_TO_DATE(STARTDATE, '%c/%e/%Y %H:%i') as STARTDATE,"
                                       "STR_TO_DATE(ENDDATE, '%c/%e/%Y %H:%i') as ENDDATE,"
                                       "PROSTATUS,"
                                       "PROFILEPICTURE,"
                                       "DISPFILE,"
                                       "ATCHTYPE,"
                                       "STR_TO_DATE(BIRTHDATE, '%Y-%m-%d %H:%i') as BIRTHDATE,"
                                       "GENDER,"
                                       "SERVICEUNIT,"
                                       "USERTYPE FROM personalreport;", self.windesk_mysql_connection, chunksize=chunk_size):
            chunk.to_sql('PERSONALREPORT', con=self.turboard_mysql_engine_connection, if_exists='append', index=False,
                         index_label='id',
                         dtype={'ID': sqlalchemy.types.Integer(),
                                'HR_CODE': sqlalchemy.types.VARCHAR(length=50),
                                'REGISTRATIONNO': sqlalchemy.types.VARCHAR(length=100),
                                'TRIDNO': sqlalchemy.types.VARCHAR(length=11),
                                'NAME': sqlalchemy.types.VARCHAR(length=100),
                                'SURNAME': sqlalchemy.types.VARCHAR(length=50),
                                'SERVICE': sqlalchemy.types.VARCHAR(length=255),
                                'COMPANY': sqlalchemy.types.VARCHAR(length=100),
                                'STARTDATE': sqlalchemy.types.DATETIME(),
                                'ENDDATE': sqlalchemy.types.DATETIME(),
                                'PROSTATUS': sqlalchemy.types.VARCHAR(length=150),
                                'PROFILEPICTURE': sqlalchemy.types.VARCHAR(length=3),
                                'DISPFILE': sqlalchemy.types.VARCHAR(length=3),
                                'ATCHTYPE': sqlalchemy.types.VARCHAR(length=150),
                                'BIRTHDATE': sqlalchemy.types.DATETIME(),
                                'GENDER': sqlalchemy.types.VARCHAR(length=150),
                                'SERVICEUNIT': sqlalchemy.types.VARCHAR(length=255),
                                'USERTYPE': sqlalchemy.types.VARCHAR(length=150)
                                })
            batch_no += 1
        print('Rows inserted ', (batch_no * chunk_size))

    def update_turboard_table_kesinti(self):
        total_start_time = time.time()
        self.turboard_mysql_engine_connection.execute(
            f"UPDATE Imported.ISSUEREPORT SET KESINTI_ORANI = NULL WHERE ID NOT IN (SELECT * FROM(SELECT MIN(ID) AS ID FROM Imported.ISSUEREPORT GROUP BY CODE) a)")
        total_end_time = time.time()
        print("Turboard issuereport table column(Kesinti Oranı) updated!")
        print("Update completed with(time) : ", total_end_time - total_start_time)

    def update_turboard_table_hata(self):
        total_start_time = time.time()
        self.turboard_mysql_engine_connection.execute(
            f"UPDATE Imported.ISSUEREPORT SET HATA_PUANI = NULL WHERE ID NOT IN (SELECT * FROM(SELECT MIN(ID) AS ID FROM Imported.ISSUEREPORT GROUP BY CODE) a)")
        total_end_time = time.time()
        print("Turboard issuereport table column(Hata Oranı) updated!")
        print("Update completed with(time) : ", total_end_time - total_start_time)


def main():
    total_start_time = time.time()
    transform_data = Transform()
    # transform_data.truncate_turboard_table()
    # transform_data.write_issuereport_data()
    # transform_data.write_callreport_data()
    transform_data.write_personalreport_data()
    # transform_data.update_turboard_table_kesinti()
    # transform_data.update_turboard_table_hata()
    total_end_time = time.time()
    print("Data Transform Completed with(time) : ", total_end_time - total_start_time)


if __name__ == '__main__':
    main()
    sys.exit()
