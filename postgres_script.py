import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname='socialmedia', user='postgres', host='/tmp')
c = conn.cursor()

timestamp = datetime.today().strftime("%s")

c.execute(
    '''CREATE TABLE logins_7d_%s AS
    SELECT userid, COUNT(*) AS cnt
    FROM logins
    WHERE logins.tmstmp > timestamp '2014-08-14' - interval '7 days'
    GROUP BY userid;''' % timestamp
)

conn.commit()
conn.close()