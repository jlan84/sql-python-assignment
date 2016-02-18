import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname='socialmedia', user='postgres', host='/tmp')
c = conn.cursor()

today = '2014-08-14'

timestamp = datetime.strptime(today, '%Y-%m-%d').strftime("%Y%m%d")
c.execute('''CREATE TABLE logins_7d AS
    SELECT userid, COUNT(*) AS cnt, timestamp %(timestamp)s AS date_7d
    FROM logins
    WHERE logins.tmstmp > timestamp %(timestamp)s - interval '7 days'
    GROUP BY userid;''', {'timestamp': timestamp}
)

conn.commit()
conn.close()
