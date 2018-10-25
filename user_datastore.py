import sqlite3 as sql
import time
import tempfile
import uuid

alert_default=""

status_enum = ['WRONG_PASSWORD', 'NO_SUCH_USER', 'SUCCESS']

WRONG_PASSWORD=0
NO_SUCH_USER=1
SUCCESS=2

class UserDatabase(object):

    def __init__(self, db_name = 'user_config.db'):
        if db_name[0] != '/':
            temp_log_dir = tempfile.mkdtemp(prefix = 'user-', dir='/tmp')
            db_name = temp_log_dir+'/'+db_name
        self.db_name = db_name
        self.conn = sql.connect(db_name)
        init_users = 'CREATE TABLE IF NOT EXISTS users (Name TEXT, Email TEXT, Phone TEXT, PasswordHash BLOB, PasswordMeta BLOB, NodeID TEXT, Alerts BLOB, AppSettings BLOB)'
        self.conn.execute(init_users)
        create_index = 'CREATE INDEX IF NOT EXISTS users_index on users (Email ASC)'
        self.conn.execute(create_index)
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def check_user(self, email, password_hash):
        selector = 'SELECT * FROM users WHERE email=?'
        users = self.cursor.execute(selector, (email,)).fetchall()
        if users:
            name, email, phone, password_hash_stored, password_meta_stored, node_id, alerts, app_settings = users[0]
            if any([x != y for (x,y) in zip(password_hash_stored, password_hash)]):
                return WRONG_PASSWORD
            else:
                return SUCCESS
        return NO_SUCH_USER

    def get_user_settings(self, email, password_hash):
        selector = 'SELECT * FROM users WHERE email=?'
        user = self.cursor.execute(selector, (email,)).fetchone()
        name, email, phone, password_hash_stored, password_meta_stored, node_id, alerts, app_settings = user
        if False not in [x == y for (x,y) in zip(password_hash_stored, password_hash)]:
            return (name, email, phone, node_id, alerts, app_settings)

    def update_user(self, *args, **kwargs):
        raise NotImplemented

    def update_alerts(self, email, alerts):
        command = "UPDATE users SET Alerts=? WHERE email=?"
        return self.cursor.execute(command, (json.dumps(alerts), email))

    def get_users_and_alerts(self):
        selector = "SELECT (UID, NodeID, Alerts) FROM users"
        alerts = self.cursor.execute(selector)
        return alerts

    def update_app_settings(self):
        raise NotImplemented
        # localization, units, etc

    def add_user(self, name, email, phone, password_hash, password_meta, node_id, alert_defaults = '', app_settings = ''):
        inserter = "INSERT INTO users VALUES(?, ?, ?, ?, ?, ?, '', '')"
        data = (name, email, phone, password_hash, password_meta, node_id)
        self.cursor.executemany(inserter, [data])
        self.conn.commit()
        return SUCCESS
