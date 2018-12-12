from dataclasses import dataclass
import sqlite3 as sql
import time
import tempfile
import uuid
from enum import Enum

alert_default = ""


class Status(Enum):
    WRONG_PASSWORD = 0
    NO_SUCH_USER = 1
    SUCCESS = 2


@dataclass
class User:
    name: str
    email: str
    phone: str
    password_hash: bytes
    password_meta: str
    node_id: str
    alerts: bytes
    app_settings: bytes


class UserDatabase(object):
    def __init__(self, db_name='user_config.db'):
        if db_name[0] != '/':
            temp_log_dir = tempfile.mkdtemp(prefix='user-', dir='/tmp')
            db_name = temp_log_dir + '/' + db_name
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
        users = self.cursor.execute(selector, (email, )).fetchall()
        if users:
            name, email, phone, password_hash_stored, password_meta_stored, node_id, alerts, app_settings = users[0]
            if any([x != y for (x, y) in zip(password_hash_stored, password_hash)]):
                return Status.WRONG_PASSWORD, None
            else:
                return Status.SUCCESS, User(*users[0])
        return Status.NO_SUCH_USER, None

    def get_user_settings(self, email, password_hash):
        selector = 'SELECT * FROM users WHERE email=?'
        user = User(*self.cursor.execute(selector, (email, )).fetchone())
        if all([x == y for (x, y) in zip(user.password_hash, password_hash)]):
            return user

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

    def add_user(self, name, email, phone, password_hash, password_meta, node_id, alert_defaults='', app_settings=''):
        inserter = "INSERT INTO users VALUES(?, ?, ?, ?, ?, ?, '', '')"
        data = (name, email, phone, password_hash, password_meta, node_id)
        self.cursor.executemany(inserter, [data])
        self.conn.commit()
        return SUCCESS
