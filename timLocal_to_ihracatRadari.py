import sys
import time
import logging

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder


def local_connection():
    try:
        engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('********',
                                                                                                  '***********',
                                                                                                  '**********',
                                                                                                  5433,
                                                                                                  '***********'))

        return engine
    except Exception as e:
        print(e)


def prod_connection():
    try:
        vertica_server = SSHTunnelForwarder(
            '**********',
            ssh_username="********",
            ssh_password="***********",
            remote_bind_address=("************", 5434)
        )
        vertica_server.start()

        engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('***********',
                                                                                                  '**********',
                                                                                                  '**********',
                                                                                                  vertica_server.local_bind_port,
                                                                                                  '***********'))

        return engine
    except Exception as e:
        print(e)


class Transform:
    def __init__(self):
        print("Starting: ")
        self.local_connection = local_connection()
        print("Connected successfully(Local)!")
        self.prod_connection = prod_connection()
        print("Connected successfully(Prod)!")
        self.count_local_data = self.count_local_data()
        self.connection = local_connection().connect()

    def count_local_data(self):
        try:
            query = "select count(*) from tim.tim_ulkelerarasi_ticaret"
            df = self.local_connection.execute(query)
            df_final = pd.DataFrame(df)
            if df_final.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
                exit()
            else:
                print('tim_ulkelerarasi_ticaret row count => ' + str(df_final.values[0][0]))
                logging.info('tim_ulkelerarasi_ticaret row count => ' + str(df_final.values[0][0]))
            return df_final.values[0][0]
        except Exception as e:
            print(e)

    def count_prod_data(self):
        try:
            query = "select count(*) from tim_ulkelerarasi_ticaret_transform"
            df = self.prod_connection.execute(query)
            df_final = pd.DataFrame(df)
            if df_final.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
                exit()
            return df_final.values[0][0]
        except Exception as e:
            print(e)

    def insert_prod_data(self):
        print("inserting...")
        table_size = 50000000
        try:
            query = "select * from tim.tim_ulkelerarasi_ticaret"
            batch_no = 0
            for chunk in pd.read_sql_query(query, self.connection, chunksize=10000):
                chunk.to_sql('tim_ulkelerarasi_ticaret_transform', con=self.prod_connection, if_exists='append',
                             index=False,
                             index_label='id', schema='public'
                             , dtype={'cmdCode6': sqlalchemy.types.VARCHAR(length=300),
                                      'cmdDesc2': sqlalchemy.types.VARCHAR(length=500),
                                      'cmdDesc4': sqlalchemy.types.VARCHAR(length=500),
                                      'cmdDesc6': sqlalchemy.types.VARCHAR(length=500),
                                      'rtCode': sqlalchemy.types.Numeric(),
                                      'rtTitle': sqlalchemy.types.VARCHAR(length=500),
                                      'ptCode': sqlalchemy.types.Numeric(),
                                      'ptTitle': sqlalchemy.types.VARCHAR(length=500),
                                      'rgCode': sqlalchemy.types.Numeric(),
                                      'rgDesc': sqlalchemy.types.VARCHAR(length=500),
                                      'yr_as_date': sqlalchemy.types.Date(),
                                      'yr': sqlalchemy.types.Numeric(),
                                      'TradeValue': sqlalchemy.types.Numeric(),
                                      'qtCode': sqlalchemy.types.Numeric(),
                                      'qtDesc': sqlalchemy.types.VARCHAR(length=500),
                                      'TradeQuantity': sqlalchemy.types.Numeric(),
                                      'NetWeight': sqlalchemy.types.Numeric(),
                                      'is_reverse': sqlalchemy.types.Numeric(),
                                      'year_as_varchar': sqlalchemy.types.VARCHAR(
                                          length=4)}
                             )
                batch_no += 1
                print('index: {0} / current table size : {1}'.format(batch_no, self.count_prod_data()))
                if self.count_prod_data() >= table_size:
                    self.connection = local_connection().connect()
                    print("local connection reset")
                    table_size = table_size + 50000000
            print("Inserted row count : " + str(self.count_local_data))
            logging.info("Inserted row count : " + str(self.count_local_data))
            self.prod_connection.execute("COMMIT;")
        except Exception as e:
            print(e)
            logging.error(e)
            self.prod_connection.execute("ROLLBACK;")


def main():
    transform_data = Transform()
    total_start_time = time.time()
    logging.basicConfig(filename='tim_ulkelerarasi_ticaret.log', level=logging.WARNING,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.warning("Started whole process")

    print("****************************************************")

    print("inserting to table...")
    logging.warning("inserting to table...")
    insert_start_time = time.time()
    transform_data.insert_prod_data()
    logging.warning("Data inserted")
    insert_end_time = time.time()
    print("Insert completed with(time) : " + str(insert_end_time - insert_start_time))
    logging.warning("Insert completed with(time) : " + str(insert_end_time - insert_start_time))
    # transform_data.update_task_position()
    # logging.warning("Done column of tasks updated!")

    print("****************************************************")

    total_end_time = time.time()
    print("Transform completed with(time) : " + str(total_end_time - total_start_time))
    logging.warning("Transform completed with(time) : " + str(total_end_time - total_start_time))


if __name__ == '__main__':
    main()
    logging.warning("All process completed. ")
    sys.exit()
