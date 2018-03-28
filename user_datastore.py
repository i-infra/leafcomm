import sqlite3 as sql
import time
import tempfile
import uuid

alert_default=""

class UserDatabase(object):

    def __init__(self, db_name = 'user_config.db'):
        if db_name[0] != '/':
            temp_log_dir = tempfile.mkdtemp(prefix = 'user-', dir='/tmp')
            db_name = temp_log_dir+'/'+db_name
        self.db_name = db_name
        self.conn = sql.connect(db_name)
        init_users = 'CREATE TABLE IF NOT EXISTS users (UID INTEGER PRIMARY KEY, Name TEXT, \
                Email TEXT, Phone TEXT, PasswordHash BLOB, PasswordMeta BLOB, NodeID TEXT, Alerts BLOB)'
        self.conn.execute(init_users)
        create_index = 'CREATE INDEX IF NOT EXISTS ON users (Email ASC)'
        self.conn.execute(create_index)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def check_user(self, email, password_hash):
        if stop == -1:
            stop = time.time()
        selector = 'SELECT * FROM users WHERE Email is %s' % str(email)
        users = self.cursor.execute(selector)
        if users:
            uid, name, email, phone, password_hash_stored, password_meta_stored, alerts = users[0]
            if False in [x == y for (x,y) in zip(password_hash_stored, password_hash)]:
                return None
            else:
                return User(name, email, phone, alerts)

    def update_user(self, *args, **kwargs):
        raise NotImplemented

    def update_alerts(self, email, alerts):
        command = "UPDATE users SET Alerts = '%s' WHERE email = '%s';" % (json.dumps(alerts), email)
        return self.cursor.execute(command)

    def get_users_and_alerts(self):
        selector = "SELECT (UID, NodeID, Alerts) FROM users"
        alerts = self.cursor.execute(selector)
        return alerts

    def add_user(self, name, email, phone, password_hash, password_meta):
        inserter = "INSERT INTO readings VALUES(?, ?, ?, ?, ?)"
        data = (name, email, phone, password_hash, password_meta, alert_defaults)
        self.cursor.executemany(inserter, [data])
        return self.conn.commit()
