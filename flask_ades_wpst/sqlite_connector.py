import os
import json
import sqlite3
import requests
import yaml
from datetime import datetime, timezone


class SQLiteConnector():

    def __init__(self, db_name="./sqlite/sqlite.db"):
        self._db_name = db_name

    def _create_connection(self, db_file):
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

    def _create_table(self, conn, create_table_sql):
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

    def _sqlite_get_headers(self, cur, tname):
        sql_str = 'SELECT name FROM PRAGMA_TABLE_INFO("{}");'.format(tname)
        cur.execute(sql_str)
        col_headers = [t[0] for t in cur.fetchall()]
        return col_headers

    def sqlite_db(func):
        from functools import wraps
        @wraps(func)
        def wrapper_sqlite_db(self, *args, **kwargs):
            init_table = not os.path.exists(self._db_name)
            conn = self._create_connection(self._db_name)
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
                                             jobOwner TEXT,
                                             procID TEXT,
                                             inputs DATA,
                                             backend_info DATA,
                                             metrics DATA,
                                             status TEXT,
                                             timeCreated TEXT,
                                             timeUpdated TEXT
                                           );"""
                self._create_table(conn, sql_create_procs_table)
                self._create_table(conn, sql_create_jobs_table)
            return func(self, *args, **kwargs)
        return wrapper_sqlite_db

    @sqlite_db
    def sqlite_get_procs(self):
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = "SELECT * FROM processes"
        cur.execute(sql_str)
        return cur.fetchall()

    @sqlite_db
    def sqlite_get_proc(self, proc_id):
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = """SELECT * FROM processes
                     WHERE id = \"{}\"""".format(
            proc_id
        )
        cur.execute(sql_str)
        res = cur.fetchall()
        num_matches = len(res)
        if num_matches == 1:
            res = res[0]
        else:
            assert (
                num_matches == 0,
                "Found more than one match for {}. This should never happen.".format(
                    proc_id
                ),
            )
        return res

    @sqlite_db
    def sqlite_deploy_proc(self, proc_spec):
        proc_desc = proc_spec["processDescription"]
        proc_desc2 = proc_desc["process"]
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = """INSERT INTO processes(id, title, abstract, keywords, 
                                           owsContextURL, processVersion, 
                                           jobControlOptions,
                                           outputTransmission,
                                           immediateDeployment, executionUnit)
                     VALUES(\"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", 
                            \"{}\", \"{}\", \"{}\", \"{}\");""".format(
            proc_desc2["id"],
            proc_desc2["title"],
            proc_desc2["abstract"],
            ",".join(proc_desc2["keywords"]),
            proc_desc2["owsContext"]["offering"]["content"]["href"],
            proc_desc["processVersion"],
            ",".join(proc_desc["jobControlOptions"]),
            ",".join(proc_desc["outputTransmission"]),
            int(proc_spec["immediateDeployment"]),
            ",".join([d["href"] for d in proc_spec["executionUnit"]]),
        )
        cur.execute(sql_str)
        conn.commit()
        return self.sqlite_get_proc(proc_desc2["id"])

    @sqlite_db
    def sqlite_undeploy_proc(self, proc_id):
        proc_desc = self.sqlite_get_proc(proc_id)
        if proc_desc:
            conn = self._create_connection(self._db_name)
            cur = conn.cursor()
            sql_str = """DELETE FROM processes
                         WHERE id = \"{}\"""".format(
                proc_id
            )
            cur.execute(sql_str)
            conn.commit()
        return proc_desc

    @sqlite_db
    def sqlite_get_jobs(self, proc_id=None):
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = "SELECT * FROM jobs"
        if proc_id is not None:
            sql_str += """ WHERE procID = \"{}\"""".format(proc_id)
        cur.execute(sql_str)
        job_list = cur.fetchall()
        col_headers = self._sqlite_get_headers(cur, "jobs")
        job_dicts = [dict(zip(col_headers, job)) for job in job_list]
        return job_dicts

    @sqlite_db
    def sqlite_get_job(self, job_id):
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = """SELECT * FROM jobs
                     WHERE jobID = \"{}\"""".format(
            job_id
        )
        job_matches = cur.execute(sql_str).fetchall()
        num_matches = len(job_matches)
        if num_matches == 0:
            job_dict = {}
        elif num_matches == 1:
            job = job_matches[0]
            col_headers = self._sqlite_get_headers(cur, "jobs")
            job_dict = {}
            for i, col in enumerate(col_headers):
                # deserialize JSON data fields
                if col in ("inputs", "backend_info", "metrics"):
                    job_dict[col] = json.loads(job[i])
                else:
                    job_dict[col] = job[i]
        else:
            # This should never happen.
            raise ValueError("Found more than one match for job ID {}".format(job_id))
        return job_dict

    @sqlite_db
    def sqlite_exec_job(self, proc_id, job_id, job_spec, job_owner,
                        backend_info):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO jobs(jobID, jobOwner, procID, inputs, backend_info, metrics, status, timeCreated, timeUpdated)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                job_id,
                job_owner,
                proc_id,
                json.dumps(job_spec),
                json.dumps(backend_info),
                "{}",
                "accepted",
                now,
                now
            ],
        )
        conn.commit()
        return self.sqlite_get_job(job_id)

    @sqlite_db
    def sqlite_update_job_status(self, job_id, status, metrics):
        conn = self._create_connection(self._db_name)
        cur = conn.cursor()
        sql_str = """UPDATE jobs
                     SET status = \"{}\",
                         metrics = \'{}\',
                         timeUpdated = \"{}\"
                     WHERE jobID = \"{}\"""".format(
            status,
            json.dumps(metrics),
            datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z"),
            job_id,
        )
        cur.execute(sql_str)
        conn.commit()
        return self.sqlite_get_job(job_id)

    @sqlite_db
    def sqlite_dismiss_job(self, job_id):
        job_dict = self.sqlite_get_job(job_id)
        if job_dict:
            resp = self.sqlite_update_job_status(job_id, "dismissed", {})
        else:
            resp = {}
        return resp
