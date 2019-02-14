from dataclasses import dataclass, asdict
import sqlite3 as sql
import time
import tempfile
import typing
from enum import Enum
import nacl.pwhash

from enforce_types import enforce_types


class Status(int, Enum):
    WRONG_PASSWORD = 0
    NO_SUCH_USER = 1
    SUCCESS = 2


class AlertMechanism(int, Enum):
    EMAIL = 0
    SMS = 1


class Direction(int, Enum):
    RISING = +1
    FALLING = -1


@enforce_types
@dataclass
class Alert:
    name: str
    enabled: bool
    temp: float
    direction: Direction
    method: AlertMechanism
    user_contact_info: str
    minimum_duration: int
    sensor_uids: typing.List[int]
    last_updated_timestamp: float


@enforce_types
@dataclass
class User:
    name: str
    email: str
    phone: str
    password_hash: bytes
    password_meta: str
    node_id: str
    alerts: str
    app_settings: str


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

    def check_user(self, email, client_password_hash):
        selector = 'SELECT * FROM users WHERE email=?'
        users = self.cursor.execute(selector, (email, )).fetchall()
        if users:
            name, email, phone, password_hash_stored, password_meta_stored, node_id, alerts, app_settings = users[0]
            if nacl.pwhash.verify(password_hash_stored, client_password_hash):
                return Status.SUCCESS, User(*users[0])
            else:
                return Status.WRONG_PASSWORD, None
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

    def add_user(self, name, email, phone, first_password_hash, password_meta, node_id, alert_defaults='', app_settings=''):
        inserter = "INSERT INTO users VALUES(?, ?, ?, ?, ?, ?, '', '')"
        password_hash = nacl.pwhash.str(first_password_hash)
        data = (name, email, phone, password_hash, password_meta, node_id)
        self.cursor.executemany(inserter, [data])
        self.conn.commit()
        return Status.SUCCESS
