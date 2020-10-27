import psycopg2
from datetime import datetime



if __name__ == "__main__":
        
    conn = psycopg2.connect(dbname='socialmedia', user='postgres', host='localhost',
                            password='password')
    c = conn.cursor()
    today = '2014-08-14'
    ts = datetime.strptime(today, '%Y-%m-%d').strftime("%Y%m%d")
    print(ts, today)

    # c.execute('''
    #             CREATE TABLE logins_7d AS
    #             SELECT userid, COUNT(*) AS cnt, timestamp %(ts)s AS date_7d
    #             FROM logins
    #             WHERE logins.tmstmp > timestamp %(ts)s - interval '7 days'
    #             GROUP BY userid;''', {'ts':ts}
    #             )




    c.execute('''
            CREATE TEMPORARY TABLE last_login AS
            SELECT userid, MAX(tmstmp) AS last_login
            FROM logins as l
            WHERE l.tmstmp <= %(ts)s
            GROUP BY userid;
            
            CREATE TEMPORARY TABLE mobile_logins AS
            SELECT userid, COUNT(*) AS cnt, timestamp %(ts)s AS date_7d
            FROM logins AS l
            WHERE l.tmstmp > timestamp %(ts)s - interval '7 days' AND
                    l.type = 'mobile'
            GROUP BY userid;


            CREATE TEMPORARY TABLE web_logins AS
            SELECT userid, COUNT(*) AS cnt, timestamp %(ts)s AS date_7d
            FROM logins AS l
            WHERE l.tmstmp > timestamp %(ts)s - interval '7 days' AND
                    l.type = 'web'
            GROUP BY userid;


            CREATE TEMPORARY TABLE user_opt_out AS
            SELECT r.userid, CASE WHEN r.userid = o.userid THEN 'YES' 
            ELSE 'NO' END AS opt_out
            FROM registrations AS r
            LEFT JOIN optout AS o
            ON r.userid = o.userid
            GROUP BY r.userid, o.userid;
            
            CREATE TABLE snapshot AS
            SELECT r.userid, r.tmstmp as reg_date,
            COALESCE(l7d.cnt, 0) AS login_7d,
            COALESCE(l7d_m.cnt, 0) AS login_7d_mobile,
            COALESCE(l7d_w.cnt, 0) AS login_7d_web,
            opt.opt_out AS opt_out, ll.last_login AS last_login
            FROM registrations r
            LEFT JOIN last_login AS ll ON ll.userid = r.userid
            LEFT JOIN logins_7d AS l7d ON l7d.userid = r.userid
            LEFT JOIN mobile_logins AS l7d_m ON l7d_m.userid = r.userid
            LEFT JOIN web_logins AS l7d_w ON l7d_w.userid = r.userid
            LEFT JOIN user_opt_out AS opt ON opt.userid = r.userid
            ORDER BY r.userid;
            ''', {'ts':ts}
            )
    
    conn.commit()
    conn.close()
