from pyhive import hive

conn = hive.connect(
    host='hadoop8',
    port=10000,
    username='root',
    password='hive',
    database='special_database',
    auth='CUSTOM'
)

cursor = conn.cursor()

cursor.execute('show tables')

for result in cursor.fetchall():
    print(result)