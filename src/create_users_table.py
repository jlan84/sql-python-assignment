#!/Users/mark/anaconda/bin/python

import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname='socialmedia', user='postgres', host='localhost',password="password")
c = conn.cursor()

# today = datetime.now()
today = '2014-08-14'

timestamp = datetime.strptime(today, '%Y-%M-%d').strftime("%s")

c.execute(
    '''CREATE TABLE users_7d_{0} AS (
WITH
    userOptOut AS (
    SELECT r.userid,
        CASE WHEN (o.userid IS NOT NULL) THEN 'TRUE' ELSE 'FALSE' END AS optout
	FROM
	    registrations r LEFT OUTER JOIN optout o
	    ON r.userid = o.userid
	ORDER BY r.userid),
    userLastLogin AS (
    SELECT userid, MAX(date(tmstmp)) as lastlogin
        FROM logins
        GROUP BY userid
	ORDER BY userid),
    last7logins AS (
    SELECT userid, count(1) as logins_7d,
           COUNT(CASE type WHEN 'web' THEN 1 ELSE null END) AS logins_7d_web,
           COUNT(CASE type WHEN 'mobile' THEN 1 ELSE null END)
	     AS logins_7d_mobile
        FROM logins
	WHERE DATE(tmstmp) BETWEEN
	    (to_date('{1}', 'YYYY-MM-DD') - 7)
	    AND to_date('{2}', 'YYYY-MM-DD')
	GROUP BY userid
	ORDER BY userid),
    pals AS (
    SELECT r.userid, count(1) as num_friends
         FROM registrations r LEFT OUTER JOIN friends f
	 ON (r.userid = f.userid1)
	    OR (r.userid = f.userid2)
	 GROUP BY r.userid
	 ORDER BY r.userid)
SELECT r.userid, DATE(r.tmstmp) as reg_date, DATE(l.lastlogin) as last_login,
       s.logins_7d, s.logins_7d_mobile, s.logins_7d_web, p.num_friends, o.optout
    FROM
        (((registrations r JOIN userOptOut o
           ON r.userid = o.userid)
	   JOIN userLastLogin l
           ON r.userid = l.userid)
           JOIN last7logins s
           ON r.userid = s.userid)
	   JOIN pals p
	   on r.userid = p.userid
ORDER BY r.userid);'''.format(timestamp, today, today))

conn.commit()
conn.close()