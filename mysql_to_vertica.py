import pandas as pd
import MySQLdb
#import vertica_python
import sqlalchemy

vertica_conn_info = {'host':'*********', 'port':5433, 'user':'**********', 'password':'**********', 'database':'*************'}

mysql_conn_info = {'host':'*********', 'user':'**********', 'passwd':'***********', 'db':'*********', 'port':3306}

mysql_connection = MySQLdb.connect(**mysql_conn_info)

#vertica_connection = vertica_python.connect(**vertica_conn_info)

database_connection = sqlalchemy.create_engine('vertica+vertica_python://{0}:{1}@{2}/{3}'.
                                                   format('************', '************',
                                                          '***************', '***************'))

df = pd.read_sql_query("select * from table_name", mysql_connection)
  
#df.to_sql('table_name', con=database_connection, if_exists='append', index=False, index_label='macmap_id', dtype={'main_country': sqlalchemy.types.VARCHAR(length=255),
#                                                           'gtip_code': sqlalchemy.types.VARCHAR(length=255), 'type': sqlalchemy.types.VARCHAR(length=255),
#                                                           'tariff_regime': sqlalchemy.types.VARCHAR(length=255), 'tariff_reported_unit': sqlalchemy.types.VARCHAR(length=255),
#                                                           'tariff_reported_standart_unit': sqlalchemy.types.VARCHAR(length=255), 'tariff_average_unit': sqlalchemy.types.VARCHAR(length=255)})

df.to_sql('table_name', con=database_connection, if_exists='append', index=False, index_label='id', dtype={'country_name': sqlalchemy.types.VARCHAR(length=200), 'state_name': sqlalchemy.types.VARCHAR(length=200), 'state': sqlalchemy.types.VARCHAR(length=200), 'address': sqlalchemy.types.VARCHAR(length=3000), 'phone': sqlalchemy.types.VARCHAR(length=2000), 'fax': sqlalchemy.types.VARCHAR(length=2000), 'email': sqlalchemy.types.VARCHAR(length=200), 'mission_land': sqlalchemy.types.VARCHAR(length=2000)})
  
print("Successfully transfered...")

