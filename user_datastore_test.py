import user_datastore
import nacl

test_datastore = user_datastore.UserDatabase()

test_datastore.add_user('alfred hitchcock', 'ahitchock@gmail.com', '+15133278483', nacl.password_hash
