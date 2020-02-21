import psycopg2
from psycopg2 import sql
from datetime import datetime


def navigate(tab, curs, direct,count):
    records=0
    if direct == 'DOWN':
        cursor.execute(sql.SQL("SELECT * FROM {} WHERE date >= to_timestamp(%s) order by date limit %s").format(sql.Identifier(tab)), (curs, count))
        records = cursor.fetchall()
    elif direct == 'UP':
        cursor.execute(sql.SQL("SELECT * FROM {} WHERE date <= to_timestamp(%s) order by date desc limit %s").format(sql.Identifier(tab)), (curs, count))
        records = cursor.fetchall()
    else:
        print("Неверный ввод")
    return records


conn = psycopg2.connect(dbname='univer', user='user_test', password='test', host='localhost')
cursor = conn.cursor()
t = input("Название таблицы")
a = input("С какой записи?")
dir = input("Направление движения")
b = int(input("Сколько записей?"))
d = datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
ts = d.timestamp()
# cursor.execute("""
#     INSERT INTO table1(id1,date,task_id,name,i)
#     SELECT s.id,random()*(NOW()-timestamp'2018-01-01 00:00:00')+timestamp '2018-01-01 00:00:00',floor(random()*(300-200)+200)::integer,chr((32+random()*94)::integer), random() < 0.01
#     FROM generate_series(1,100000) as s(id);
# """)
conn.commit()
print(navigate(t,ts,dir,b))
cursor.close()
conn.close()
