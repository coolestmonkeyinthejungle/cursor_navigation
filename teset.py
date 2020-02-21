import psycopg2
from psycopg2 import sql
from datetime import datetime
import threading
import time


out = []
lock = threading.Lock()


def sortByDate(inputData):
    return inputData[1]


def writerf(tab, curs, direct):
    lock.acquire()
    try:
        l = filter(tab, curs, direct)
        for i in range(len(l)):
            out.append(l[i])
    finally:
        lock.release()


def writera(tab, direct):
    lock.acquire()
    try:
        l = data(tab, direct)
        for i in range(len(l)):
            out.append(l[i])
    finally:
        lock.release()


def filter(tab, curs, direct):
    records=0
    if direct == 'Yes':
        cursor.execute(sql.SQL("SELECT * FROM {} WHERE date >= to_timestamp(%s) order by date").format(sql.Identifier(tab)), (curs, ))
        records = cursor.fetchall()
    elif direct == 'No':
        cursor.execute(sql.SQL("SELECT * FROM {} WHERE date <= to_timestamp(%s) order by date desc").format(sql.Identifier(tab)), (curs, ))
        records = cursor.fetchall()
    else:
        print("Неверный ввод")
    return records


def data(tab,direct):
    records=0
    if direct == 'Yes':
        cursor.execute(sql.SQL("SELECT * FROM {} order by date").format(sql.Identifier(tab)))
        records = cursor.fetchall()
    elif direct == 'No':
        cursor.execute(sql.SQL("SELECT * FROM {} order by date desc").format(sql.Identifier(tab)))
        records = cursor.fetchall()
    else:
        print("Неверный ввод")
    return records

def navigate(mass, count, page=1):
    for i in range(count):
        print(mass[(page-1)*count+i])
conn = psycopg2.connect(dbname='univer', user='user_test', password='test', host='localhost')
cursor = conn.cursor()
tablel = ["table1", "table2", "table3", "table4"]
tabled = {a: tablel[a] for a in range(len(tablel))}
all = input("Все записи?")
if all == "No":
    a = input("С какой записи?")
    d = datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
    ts = d.timestamp()
    dir = input("Прямая сортировка?")
    b = int(input("По сколько записей выводить?"))
    p = int(input("С какой страницы?"))
    threads = []
    ti = time.time()
    for i in range(len(tabled)):
        t = threading.Thread(target=writerf, name="Thread #%s" % i, args=(tabled[i], ts, dir))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    if dir == "Yes":
        out.sort(key=sortByDate)
    elif dir == "No":
        out.sort(key=sortByDate, reverse=True)
    print(time.time() - ti)
    navigate(out, b, p)
    exit = input("Закончить?")
    while exit != "Yes":
        p = int(input("Какая страница?"))
        navigate(out, b, p)
        exit = input("Закончить?")
elif all == "Yes":
    dir = input("Прямая сортировка?")
    b = int(input("По сколько записей выводить?"))
    p = int(input("Какая страница?"))
    threads = []
    ti = time.time()
    for i in range(len(tabled)):
        t = threading.Thread(target=writera, name="Thread #%s" % i, args=(tabled[i], dir))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    if dir == "Yes":
        out.sort(key=sortByDate)
    elif dir == "No":
        out.sort(key=sortByDate, reverse=True)
    print(time.time()-ti)
    navigate(out, b, p)
    exit = input("Закончить?")
    while exit != "Yes":
        p = int(input("Какая страница?"))
        navigate(out, b, p)
        exit = input("Закончить?")
# cursor.execute("""
#     INSERT INTO table1(id1,date,task_id,name,i)
#     SELECT s.id,random()*(NOW()-timestamp'2019-01-01 00:00:00')+timestamp '2018-01-01 00:00:00',floor(random()*(300-200)+200)::integer,chr((32+random()*94)::integer), random() < 0.01
#     FROM generate_series(1,100000) as s(id);
# """)
# conn.commit()
cursor.close()
conn.close()
