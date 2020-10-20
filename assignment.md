# SQL Pipelines from python

- [SQL Pipelines from python](#sql-pipelines-from-python)
	- [Introduction](#introduction)
	- [Basic](#basic)
		- [Part 1: Write a simple pipeline without using OOP](#part-1-write-a-simple-pipeline-without-using-oop)
	- [Advanced](#advanced)
		- [Part 2: Complete the pipeline with OOP](#part-2-complete-the-pipeline-with-oop)

> Hints for this assignment:
>  1. keep multiple terminals running. Keep one terminal for your docker terminal/psql specifically.
>  2. Check the `conn` to make sure the `password`, `username` etc are correct. Don't waste time on such mistakes.
>  3. Try-and-error a lot
>  4. Google efficiently

## Introduction
The data for this assignment is contained in the <a href="./data/socialmedia.sql">socialmedia.sql</a> dump in the data directory. You can ingest the data into PostgreSQL following the procedures outlined in the [docker postgres guide](https://github.com/GalvanizeDataScience/docker/blob/master/reference/docker_postgres.md).

**Hint:  You can name the imported database `socialmedia` so the demo script `create_users_table.py` can run without change.**

## Basic

### Part 1: Write a simple pipeline without using OOP

**We're going to build a pipeline that creates a table that's a snapshot of the system on that given day.** In the real world, these tables would be ever changing as users register and do actions on the site. It's useful to have a snapshot of the system taken on every day.

The snapshot you are going to build ultimately will be a table with these columns:

```
userid, reg_date, last_login, logins_7d, logins_7d_mobile, logins_7d_web, opt_out
```

Here's an explanation of each column. You need to create each columns.

> Hint 1: **Pay attention to the explanation of the columns because your queries need to query/create them exactly.**

> Hint 2: **Browse the tables in `socialmedia` database to understand from which table you can get relevant information**

* `userid`: user id
* `reg_date`: registration date
* `last_login`: date of last login
* `logins_7d`: number of the past 7 days for which the user has logged in (should be a value 0-7)
* `logins_7d_mobile`: number of the past 7 days for which the user has logged in on mobile
* `logins_7d_web`: number of the past 7 days for which the user has logged in on web
* `opt_out`: whether or not the user has opted out of receiving email

These columns are a lot so spend your time on some of the simplest one. To get you jump started, we provided you with an example to create the `logins_7d`column.

Here's an example that creates a table of the number of logins for each user in the past 7 days. It is in the [src/logins_7d_example.py](src/logins_7d_example.py) file. We hardcode in today as `2014-08-14` since that's the last day of the data, and pass in the timestamp as part of a dictionary in the second argument of `c.execute`.

We are going to use the `psycopg2` module ([documentation](http://initd.org/psycopg/docs/)) to interact with the database.

> The benefit of using a dictionary is to protect our table against sql injection.

```python
import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname='socialmedia', user='postgres', host='localhost', password="password")
c = conn.cursor()

today = '2014-08-14'

# This is not strictly necessary but demonstrates how you can convert a date
# to another format
ts = datetime.strptime(today, '%Y-%m-%d').strftime("%Y%m%d")

c.execute(
    '''CREATE TABLE logins_7d AS
    SELECT userid, COUNT(*) AS cnt, timestamp %(ts)s AS date_7d
    FROM logins
    WHERE logins.tmstmp > timestamp %(ts)s - interval '7 days'
    GROUP BY userid;''', {'ts': ts}
)

conn.commit()
conn.close()
```

**Don't forget to commit and close your connection!**


After finishing running this script, you can switch to the `socialmedia` database and run `\d` to check the current relationships. You will find the following tables.


 Schema |        Name         | Type  |  Owner   |
| ------|---------------------|-------|----------|
 public | friends             | table | postgres |
 public | logins              | table | postgres |
 public | **logins_7d**          | table | postgres |
 public | messages            | table | postgres |
 public | optout              | table | postgres |
 public | registrations       | table | postgres |
 public | test_group          | table | postgres |


To continue adding other columns (**Remember that you can choose to start with the ones which look easier for you**). Here are some steps to get you started:

1. Create a script like the example above that has the userid, registration date and the last login date.

2. Now try adding an additional column.
	*HINT* Use temporary tables (we can create temporary tables a few different ways, 2 are shown below)

	```sql
	SELECT userid, tmstmp as reg_date
	INTO TEMPORARY TABLE reg_table
	FROM registrations

	CREATE TEMPORARY TABLE logins_7d_2014_08_14 AS
    SELECT userid, COUNT(*) AS cnt
    FROM logins
    WHERE logins.tmstmp > timestamp '2014-08-14' - interval '7 days'
    GROUP BY userid
	```

	The below query would get us towards the final table. Note that not every user has logged in within the last 7 days--use a left join.

	```sql
	SELECT reg.userid, reg.reg_date, login_7d.cnt as logins_7d
	FROM reg_table reg
	LEFT JOIN logins_7d_2014_08_14 login_7d
		ON reg.userid=login_7d.userid
	```
**Self-check question: Which column does the column above tries to build?**


After you finish all columns, put your script in a file called `create_users_table.py`, feel free to hardcode date at this moment.

```shell
python create_users_table.py
```

As you add the remaining fields to the snapshot table, verify the results. Ask yourself the following questions:

1. Does it produce the correct number of rows?
2. Did the values of any other fields change?
3. Check a couple values, do they make sense?

## Advanced

### Part 2: Complete the pipeline with OOP

In part 2, we are going to use OOP to reorganize our code. A [src/pipeline.py](src/pipeline.py) file is provide for you. Read and try to understand how the pipeline class works.


Here is an example of using the `Pipleine` class to restructure the `logins_7d_example.py`. It is in the [src/oop_logins_7d_example.py](src/oop_logins_7d_example.py) file.

Note that if you run the non-OOP version before, running this script will create an error saying `logins_7d` relationship already exists. That's normal.


```python
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
```

Now, create the whole pipeline for the complete user snapshot table creation.