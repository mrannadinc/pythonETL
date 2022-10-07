import pandas as pd
import MySQLdb
#import vertica_python
import sqlalchemy

vertica_conn_info = {'host':'********', 'port':5433, 'user':'********', 'password':'***********', 'database':'***********'}

mysql_conn_info = {'host':'************', 'user':'************', 'passwd':'*************', 'db':'**************', 'port':3306}

mysql_connection = MySQLdb.connect(**mysql_conn_info)

#vertica_connection = vertica_python.connect(**vertica_conn_info)

database_connection = sqlalchemy.create_engine('vertica+vertica_python://{0}:{1}@{2}/{3}'.
                                                   format('*************', '***********',
                                                          '******************', '**************'))

df = pd.read_sql_query("select id, gtip_code, country_name, tariff_year, min_ave, ntl_count, CAST(ntm_count AS DECIMAL(10,6)) as ntm_count, CAST(ntm_sps_count AS DECIMAL(10,6)) as ntm_sps_count, CAST(ntm_tbt_count AS DECIMAL(10,6)) as ntm_tbt_count from table_name limit 600000 offset 500000", mysql_connection)
  
df.to_sql('table_name', con=database_connection, if_exists='append', index=False, index_label='id', dtype={'gtip_code': sqlalchemy.types.VARCHAR(length=255), 'country_name': sqlalchemy.types.VARCHAR(length=255)})

#df.to_sql('table_name', con=database_connection, if_exists='append', index=False, index_label='country_code', dtype={'name_from_tim': sqlalchemy.types.VARCHAR(length=255), 'final_name': sqlalchemy.types.VARCHAR(length=255)})
  
print("Successfully transfered...")

