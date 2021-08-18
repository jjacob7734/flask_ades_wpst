import os
import sqlite3
from datetime import datetime

db_name = "soamc_ades.db"

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def sqlite_db(func):
    def wrapper_sqlite_db(*args, **kwargs):
        init_table = not os.path.exists(db_name)
        conn = create_connection(db_name)
        if conn is None:
            raise ValueError("Could not create the database connection.")
        if init_table:
            sql_create_procs_table = """CREATE TABLE IF NOT EXISTS processes (
                                          id TEXT PRIMARY KEY,
                                          title TEXT,
                                          abstract TEXT,
                                          keywords TEXT,
                                          owsContextURL TEXT,
                                          processVersion TEXT,
                                          jobControlOptions TEXT,
                                          outputTransmission TEXT,
                                          immediateDeployment INTEGER,
                                          executionUnit TEXT
                                        );"""
            sql_create_jobs_table = """CREATE TABLE IF NOT EXISTS jobs (
                                         jobID TEXT PRIMARY KEY,
                                         inputs BLOB,
                                         outputs BLOB,
                                         status TEXT,
                                         timestamp TEXT
                                       );"""
            create_table(conn, sql_create_procs_table)
            create_table(conn, sql_create_jobs_table)
        return func(*args, **kwargs)
    return wrapper_sqlite_db

@sqlite_db
def sqlite_get_procs():
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = "SELECT * FROM processes"
    cur.execute(sql_str)
    return cur.fetchall()

@sqlite_db
def sqlite_get_proc(proc_id):
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """SELECT * FROM processes
                 WHERE id = \"{}\"""".format(proc_id)
    cur.execute(sql_str)
    return cur.fetchall()[0]

@sqlite_db
def sqlite_deploy_proc(proc_spec):
    proc_desc = proc_spec["processDescription"]
    proc_desc2 = proc_desc["process"]
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """INSERT INTO processes(id, title, abstract, keywords, 
                                       owsContextURL, processVersion, 
                                       jobControlOptions, outputTransmission,
                                       immediateDeployment, executionUnit)
                 VALUES(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", 
                        \"{}\", \"{}\", \"{}\", \"{}\");""".\
                 format(proc_desc2["id"], proc_desc2["title"],
                        proc_desc2["abstract"],
                        ','.join(proc_desc2["keywords"]),
                        proc_desc2["owsContext"]["offering"]["content"]["href"],
                        proc_desc["processVersion"],
                        ','.join(proc_desc["jobControlOptions"]),
                        ','.join(proc_desc["outputTransmission"]),
                        int(proc_spec["immediateDeployment"]),
                        ','.join([d["href"]
                                  for d in proc_spec["executionUnit"]]))
    cur.execute(sql_str)
    conn.commit()
    return sqlite_get_proc(proc_desc2["id"])

@sqlite_db
def sqlite_undeploy_proc(proc_id):
    proc_desc = sqlite_get_proc(proc_id)
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """DELETE FROM processes
                 WHERE id = \"{}\"""".format(proc_id)
    cur.execute(sql_str)
    conn.commit()
    return proc_desc

def sqlite_get_headers(cur, tname):
    sql_str = "SELECT name FROM PRAGMA_TABLE_INFO(\"{}\");".format(tname)
    cur.execute(sql_str)
    col_headers = [t[0] for t in cur.fetchall()]
    return col_headers

@sqlite_db
def sqlite_get_jobs():
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = "SELECT * FROM jobs"
    cur.execute(sql_str)
    job_list = cur.fetchall()
    col_headers = sqlite_get_headers(cur, "jobs")
    job_dicts = [dict(zip(col_headers, job)) for job in job_list]
    return job_dicts

@sqlite_db
def sqlite_get_job(job_id):
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """SELECT * FROM jobs
                 WHERE jobID = \"{}\"""".format(job_id)
    job = cur.execute(sql_str).fetchall()[0]
    print("job=", job)
    col_headers = sqlite_get_headers(cur, "jobs")
    print("col_headers=", col_headers)
    job_dict = dict(zip(col_headers, job))
    print("job_dict=", job_dict)
    return job_dict

@sqlite_db
def sqlite_exec_job(job_id, job_spec):
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """INSERT INTO jobs(jobID, inputs, outputs, status, timestamp)
                 VALUES(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\");""".\
                 format(job_id, job_spec["inputs"], job_spec["outputs"],
                        "accepted", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    cur.execute(sql_str)
    conn.commit()
    return sqlite_get_job(job_id)

@sqlite_db
def sqlite_dismiss_job(job_id):
    conn = create_connection(db_name)
    cur = conn.cursor()
    sql_str = """UPDATE jobs
                 SET status = \"{}\",
                     timestamp = \"{}\"
                 WHERE jobID = \"{}\"""".\
                 format("dismissed",
                        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        job_id)
    cur.execute(sql_str)
    conn.commit()
    return sqlite_get_job(job_id)

