import vertica_python
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import sqlalchemy
import cx_Oracle
from datetime import datetime

vertica_server = SSHTunnelForwarder(
    '*******',
    ssh_username="**********",
    ssh_password="***********",
    remote_bind_address=('************', 5433))

vertica_server.start()
print("SSH Connection Established.")

dsn_tns = cx_Oracle.makedsn('*************', 1521, service_name='**********')
conn_oracle = cx_Oracle.connect(user='**************', password='***********', dsn=dsn_tns)

vertica_connection = sqlalchemy.create_engine('vertica+vertica_python://{0}:{1}@{2}:{3}/{4}'.
                                                       format('************', '************',
                                                              '*****************', vertica_server.local_bind_port, '*************'))
def get_table_len(table_name, connection, where_clause):

    len_df = pd.read_sql_query(r"SELECT COUNT(*) as len FROM {0}{1}".format(table_name, where_clause), connection)
    len = len_df.LEN[0]
    slice_count = int(round((len / 100000) + 0.5))
    return slice_count

def get_where_date_clause(date):
    if date is None:
        return ''
    return " WHERE TRUNC(TARIH) = DATE '{0}'".format(date)

def fill_vertica_table(table_name, date=None):
    where_clause = get_where_date_clause(date)
    slice_count = get_table_len(table_name=table_name, connection=conn_oracle, where_clause=where_clause)
    for i in range(slice_count):
        query = "SELECT * FROM {0}{2} OFFSET {1} ROWS FETCH NEXT 100000 ROWS ONLY".format(table_name, str(i*100000), where_clause)
        print("fetch started...", query)
        df = pd.read_sql_query("".join(query), conn_oracle)
        if 'IHRUID' in df.columns:
            df = df.loc[:, df.columns != 'IHRUID']
        df.to_sql('{0}_1'.format(table_name), con=vertica_connection, if_exists='append', index=False)
        print("Inserted last slice : ", str(i), df.head())
    print("done", get_table_len(table_name=table_name, connection=conn_oracle, where_clause=where_clause))

def main():
    date1 = '2020-12-20'
    date2 = '2021-01-05'
    mydates = pd.date_range(date1, date2).tolist()
    #today = datetime.today().strftime('%Y-%m-%d')
    for date in mydates:
        for table_name in ["BIRLIK", "BASKANLAR_IHRGNL", "BASKANLAR_FIRMA", "BASKANLAR_UYEGNL"]:
            fill_vertica_table(table_name=table_name, date=date)

if __name__ == '__main__':
    main()
