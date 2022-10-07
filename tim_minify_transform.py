import sys
import time
import logging

import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine


def prod_connection():
    try:
        engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('************',
                                                                                                  '',
                                                                                                  '***********',
                                                                                                  5434,
                                                                                                  '**************'))
        print("Connected successfully(Prod)!")
        return engine
    except Exception as e:
        print(e)


class Transform:
    def __init__(self):
        print("Starting: ")
        self.prod_connection = prod_connection()
        print(self.count_prod_data())

    def count_prod_data(self):
        try:
            query = "select count(*) from tim_ulkelerarasi_ticaret"
            df_final = pd.read_sql(query, self.prod_connection)
            if df_final.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
                exit()
            return df_final.values[0][0]
        except Exception as e:
            print(e)

    def insert_prod_data(self):
        print("inserting...")
        try:
            batch_no = 0
            names = ['cmdCode6', 'cmdDesc2', 'cmdDesc4', 'cmdDesc6', 'rtCode', 'rtTitle', 'ptCode', 'ptTitle', 'rgCode',
                     'rgDesc', 'yr_as_date', 'yr', 'TradeValue', 'qtCode', 'qtDesc', 'TradeQuantity', 'NetWeight',
                     'is_reverse', 'year_as_varchar']
            for chunk in pd.read_csv('minify.txt', sep='\t', error_bad_lines=False, chunksize=10000, names=names):
                # print(len(chunk))
                chunk.to_sql('tim_ulkelerarasi_ticaret', con=self.prod_connection, if_exists='append',
                             index=False,
                             index_label='id', schema='public', dtype={'cmdCode6': sqlalchemy.types.VARCHAR(length=300),
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
                                                                           length=4)})
                batch_no += 1
                print('index: {0} / current table size : {1}'.format(batch_no, self.count_prod_data()))
            self.prod_connection.commit()
        except Exception as e:
            print(e)
            logging.error(e)
            self.prod_connection.rollback()


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

    print("****************************************************")

    total_end_time = time.time()
    print("Transform completed with(time) : " + str(total_end_time - total_start_time))
    logging.warning("Transform completed with(time) : " + str(total_end_time - total_start_time))


if __name__ == '__main__':
    main()
    logging.warning("All process completed. ")
    sys.exit()
