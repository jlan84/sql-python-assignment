import psycopg2
from datetime import datetime
from pipeline import Pipeline

if __name__ == '__main__':
    conn = psycopg2.connect(dbname='socialmedia', user='postgres',
                            host='localhost', port='5432', password="password")
    today = '2014-08-14'
    ts = datetime.strptime(today, '%Y-%m-%d').strftime("%Y%m%d")

    # initialize the instance
    pipeline = Pipeline(conn)

    # add a step
    pipeline.add_step('''CREATE TABLE logins_7d AS
    SELECT userid, COUNT(*) AS cnt, timestamp %(ts)s AS date_7d
    FROM logins
    WHERE logins.tmstmp > timestamp %(ts)s - interval '7 days'
    GROUP BY userid;''',
    {'ts': ts})

    # execute and close
    pipeline.execute()
    pipeline.close()


