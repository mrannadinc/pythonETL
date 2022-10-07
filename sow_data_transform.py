import sys
import time
import logging

import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine


class Transform:
    def __init__(self):
        print("Starting: ")
        self.amip_connection = self.amip_connection()
        self.mysql_connection = self.mysql_connection()
        print("Connected successfully!")
        self.without_user_with_tag = self.without_user_with_tag()
        self.without_user_without_tag = self.without_user_without_tag()
        self.none_xp_with_tag = self.none_xp_with_tag()
        self.none_xp_without_tag = self.none_xp_without_tag()
        self.none_tags = self.none_tags()
        self.dublicate_tags = self.dublicate_tags()
        print("No constraint was found for transfer to start.")
        self.get_task_close = self.get_task_close_data()
        self.with_tag = self.get_xps_with_tag()
        self.without_tag = self.get_xps_without_tag()

    def amip_connection(self):
        try:
            connection = pymysql.connect(host='************',
                                         user='************',
                                         password='**********',
                                         db='***************')
            return connection
        except Exception as e:
            print(e)

    def mysql_connection(self):
        try:
            engine = create_engine("mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('*********',
                                                                                             '***********',
                                                                                             '**************',
                                                                                             3306,
                                                                                             '**********'))
            return engine
        except Exception as e:
            print(e)

    def without_user_with_tag(self):
        try:
            without_user_with_tag = "SELECT * FROM (" \
                                    "SELECT trim('#' from ta.name) as project, t.id as task_id, " \
                                    "username, " \
                                    "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                                    "ROUND(SUM(time_estimated),2) AS XP, " \
                                    "'kosher' as diet " \
                                    "FROM tasks t " \
                                    "left join users u on u.id = t.owner_id " \
                                    "inner join task_has_tags tt on tt.task_id = t.id " \
                                    "inner join tags ta on tt.tag_id = ta.id " \
                                    "WHERE t.column_id = 27 AND t.date_completed IS NULL " \
                                    "GROUP BY username, ta.name) a " \
                                    "where username is null "
            df = pd.read_sql_query(without_user_with_tag, self.amip_connection)
            if df.size >= 1:
                print("With tag tasks must be assigned to a user! \n")
                logging.info('With tag tasks must be assigned to a user!!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def without_user_without_tag(self):
        try:
            without_user_without_tag = "SELECT * from (" \
                                       "SELECT lower(p.identifier) as project, t.id as task_id, " \
                                       "u.username, " \
                                       "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                                       "ROUND(SUM(time_estimated),2) AS XP, " \
                                       "'kosher' as diet " \
                                       "FROM tasks t " \
                                       "left join users u on u.id = t.owner_id " \
                                       "inner join projects p on t.project_id = p.id " \
                                       "inner join columns c on t.column_id = c.id " \
                                       "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (6, 7) " \
                                       "GROUP BY t.owner_id, p.id)a " \
                                       "where username is null"
            df = pd.read_sql_query(without_user_without_tag, self.amip_connection)
            if df.size >= 1:
                print("Without tag tasks must be assigned to a user! \n")
                logging.info('Without tag tasks must be assigned to a user!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def none_xp_with_tag(self):
        try:
            none_xp_with_tag = "SELECT * FROM ( " \
                               "SELECT trim('#' from ta.name) as project, t.id as task_id, " \
                               "username, " \
                               "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                               "ROUND(SUM(time_estimated),2) AS XP, " \
                               "'kosher' as diet " \
                               "FROM tasks t,users u, task_has_tags tt, tags ta " \
                               "WHERE u.id = t.owner_id AND tt.task_id = t.id AND tt.tag_id = ta.id " \
                               "AND t.column_id = 27 AND t.date_completed IS NULL " \
                               "GROUP BY username, ta.name) a " \
                               "where XP <= 0"
            df = pd.read_sql_query(none_xp_with_tag, self.amip_connection)
            if df.size >= 1:
                print("Tasks with tag must have XP value! \n")
                logging.info('Tasks with tag must have XP value!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def none_xp_without_tag(self):
        try:
            none_xp_without_tag = "SELECT * from (" \
                                  "SELECT lower(p.identifier) as project, t.id as task_id, " \
                                  "u.username, " \
                                  "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                                  "ROUND(SUM(time_estimated),2) AS XP, " \
                                  "'kosher' as diet " \
                                  "FROM tasks t " \
                                  "inner join users u on u.id = t.owner_id " \
                                  "inner join projects p on t.project_id = p.id " \
                                  "inner join columns c on t.column_id = c.id " \
                                  "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (6, 7) " \
                                  "GROUP BY t.owner_id, p.id)a " \
                                  "where XP <= 0"
            df = pd.read_sql_query(none_xp_without_tag, self.amip_connection)
            if df.size >= 1:
                print("Tasks without tag must have XP value! \n")
                logging.info('Tasks without tag must have XP value!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def none_tags(self):
        try:
            none_tags = "SELECT t.id " \
                        "FROM tasks t " \
                        "inner join users u on u.id = t.owner_id " \
                        "left join task_has_tags tt on tt.task_id = t.id " \
                        "left join tags ta on tt.tag_id = ta.id " \
                        "WHERE t.column_id = 27 AND t.date_completed IS NULL and ta.name is null"
            df = pd.read_sql_query(none_tags, self.amip_connection)
            if df.size >= 1:
                print("Tasks must use tag! \n")
                logging.info('Tasks must use tag!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def dublicate_tags(self):
        try:
            dublicate_tags = "SELECT t.id " \
                             "FROM tasks t " \
                             "inner join users u on u.id = t.owner_id " \
                             "inner join task_has_tags tt on tt.task_id = t.id " \
                             "inner join tags ta on tt.tag_id = ta.id " \
                             "WHERE t.column_id = 27 AND t.date_completed IS NULL " \
                             "GROUP BY t.id having count(t.id) > 1"
            df = pd.read_sql_query(dublicate_tags, self.amip_connection)
            if df.size >= 1:
                print("Tasks must use one tag! \n")
                logging.info('Tasks must use one tag!')
                print("Task => ", df)
                logging.info('Task => ' + str(df))
                exit()
        except Exception as e:
            print(e)

    def get_task_close_data(self):
        try:
            close_task = "SELECT p.id as project_id, u.id as user_id, DATE(NOW()) as week_end_date, " \
                         "t.id as task_id " \
                         "FROM tasks t " \
                         "inner join users u on u.id = t.owner_id " \
                         "inner join projects p on t.project_id = p.id " \
                         "inner join columns c on t.column_id = c.id " \
                         "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (7)"
            df = pd.read_sql_query(close_task, self.amip_connection)
            if df.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
            else:
                print(df.head())
                logging.info('Closing tasks info  => ' + str(df.head()))
            return df
        except Exception as e:
            print(e)

    def get_xps_with_tag(self):
        try:
            with_tag = "SELECT trim('#' from ta.name) as project, " \
                       "username, " \
                       "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                       "ROUND(SUM(time_estimated),2) AS XP, " \
                       "'kosher' as diet " \
                       "FROM tasks t,users u, task_has_tags tt, tags ta " \
                       "WHERE u.id = t.owner_id AND tt.task_id = t.id AND tt.tag_id = ta.id " \
                       "AND t.column_id = 27 AND t.date_completed IS NULL " \
                       "GROUP BY username, ta.name"
            df = pd.read_sql_query(with_tag, self.amip_connection)
            if df.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
            else:
                print(df.head())
                logging.info('With tag => ' + str(df.head()))
            return df
        except Exception as e:
            print(e)

    def get_xps_without_tag(self):
        try:
            without_tag = "SELECT lower(p.identifier) as project, " \
                          "u.username, " \
                          "SUBDATE(current_date, WEEKDAY(current_date)) as week_start_date, " \
                          "ROUND(SUM(time_estimated),2) AS XP, " \
                          "'kosher' as diet " \
                          "FROM tasks t " \
                          "inner join users u on u.id = t.owner_id " \
                          "inner join projects p on t.project_id = p.id " \
                          "inner join columns c on t.column_id = c.id " \
                          "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (6, 7) " \
                          "GROUP BY t.owner_id, p.id "
            df = pd.read_sql_query(without_tag, self.amip_connection)
            if df.size == 0:
                print('No results matched your query.')
                logging.info('No results matched your query.')
            else:
                print(df.head())
                logging.info('Without tag => ' + str(df.head()))
            return df
        except Exception as e:
            print(e)

    def insert_with_tag_xp(self):
        print("With tag project XPs inserting...")
        try:
            self.with_tag.to_sql('XP', con=self.mysql_connection, if_exists='append', index=False,
                                 index_label='id',
                                 dtype={'project': sqlalchemy.types.VARCHAR(length=25),
                                        'username': sqlalchemy.types.VARCHAR(length=15),
                                        'week_start_date': sqlalchemy.types.DATE(),
                                        'XP': sqlalchemy.types.DECIMAL(5, 2),
                                        'diet': sqlalchemy.types.Enum()})
            logging.info("with_tag tasks : " + str(self.with_tag))
            self.mysql_connection.execute("COMMIT;")
        except Exception as e:
            print(e)
            self.mysql_connection.execute("ROLLBACK;")

    def insert_without_tag_xp(self):
        print("Without tag project XPs inserting...")
        try:
            self.without_tag.to_sql('XP', con=self.mysql_connection, if_exists='append', index=False,
                                    index_label='id',
                                    dtype={'project': sqlalchemy.types.VARCHAR(length=25),
                                           'username': sqlalchemy.types.VARCHAR(length=15),
                                           'week_start_date': sqlalchemy.types.DATE(),
                                           'XP': sqlalchemy.types.DECIMAL(5, 2),
                                           'diet': sqlalchemy.types.Enum()})
            logging.info("without_tag tasks : " + str(self.without_tag))
            self.mysql_connection.execute("COMMIT;")
        except Exception as e:
            print(e)
            self.mysql_connection.execute("ROLLBACK;")

    def update_task_position(self):
        try:
            cursor = self.amip_connection.cursor()
            update_tasks = "update tasks t " \
                           "join users u on u.id = t.owner_id " \
                           "join projects p on t.project_id = p.id " \
                           "set t.date_completed = unix_timestamp(date(now())), " \
                           "t.is_active = 0 " \
                           "where p.id in (SELECT p.id " \
                           "FROM (SELECT * from tasks) t " \
                           "inner join users u on u.id = t.owner_id " \
                           "inner join projects p on t.project_id = p.id " \
                           "inner join columns c on t.column_id = c.id " \
                           "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (7)) " \
                           "and u.id in (SELECT u.id " \
                           "FROM (SELECT * from tasks) t " \
                           "inner join users u on u.id = t.owner_id " \
                           "inner join projects p on t.project_id = p.id " \
                           "inner join columns c on t.column_id = c.id " \
                           "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (7)) " \
                           "and t.id in (SELECT t.id " \
                           "FROM (SELECT * from tasks) t " \
                           "inner join users u on u.id = t.owner_id " \
                           "inner join projects p on t.project_id = p.id " \
                           "inner join columns c on t.column_id = c.id " \
                           "WHERE t.date_completed IS NULL and p.is_active = 1 and c.title = 'Done' and c.project_id not in (7)); "
            cursor.execute(update_tasks)
            logging.info(self.get_task_close)
            self.amip_connection.commit()
        except Exception as e:
            print(e)
            self.amip_connection.rollback()


def main():
    transform_data = Transform()
    total_start_time = time.time()
    logging.basicConfig(filename='sow_xp.log', level=logging.WARNING,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Started whole process")

    print("****************************************************")

    print("XPs inserting to table...")
    insert_start_time = time.time()
    transform_data.insert_with_tag_xp()
    transform_data.insert_without_tag_xp()
    insert_end_time = time.time()
    print("Insert completed with(time) : " + str(insert_end_time - insert_start_time))
    logging.info("Insert completed with(time) : " + str(insert_end_time - insert_start_time))
    transform_data.update_task_position()
    logging.info("Done column of tasks updated!")

    print("****************************************************")

    total_end_time = time.time()
    print("Transform completed with(time) : " + str(total_end_time - total_start_time))
    logging.info("Transform completed with(time) : " + str(total_end_time - total_start_time))


if __name__ == '__main__':
    main()
    logging.info("All processes were completed. ")
    sys.exit()
