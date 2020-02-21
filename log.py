import psycopg2


conn = psycopg2.connect(dbname='univer', user='user_test', password='test', host='localhost')
cursor = conn.cursor()
cursor.execute('SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables ORDER BY n_tup_ins DESC;')
print(cursor.fetchall())
