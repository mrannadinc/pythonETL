import psycopg

hostname = "localhost"
database = "postgres"
username = "postgres"
pwd = "512154"
port_id = 5432
conn = None
cur = None

try:
    with psycopg.connect(
                host = hostname, 
                dbname = database,
                user = username,
                password = pwd,
                port = port_id) as conn:

        with conn.cursor() as cur:

            drop_table = 'DROP TABLE individual'

            cur.execute(drop_table)

            create_table_script = ''' CREATE TABLE IF NOT EXISTS individual (
                                            id varchar(255) PRIMARY KEY,
                                            gender  varchar(255) NOT NULL)'''
            
            cur.execute(create_table_script)

            insert_table_script = 'INSERT INTO individual (id, gender) VALUES (%s, %s)'
            insert_value = ('eea547a3-06fd-4d13-acf7-ee3cdc3c4120','MALE')
            cur.execute(insert_table_script, insert_value)

            select_table = 'SELECT * FROM individual'
            cur.execute(select_table)
            for record in cur.fetchall():
                print(record[0],record[1])

            conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
