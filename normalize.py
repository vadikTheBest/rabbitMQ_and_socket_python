import sqlite3
import sqlite3 as lite
import sys

def read_tb(name_db, name_tb):
    con = lite.connect(name_db)
    with con:    
        cur = con.cursor()    
        cur.execute(f"SELECT * FROM {name_tb}")
        rows = cur.fetchall()
        for row in rows:
            print(row)    
def normalize_bd():
    conn_1db = sqlite3.connect("new_db.db") 
    cursor_1db = conn_1db.cursor()
    conn_2db = sqlite3.connect("normalize_db.db") 
    cursor_2db = conn_2db.cursor()
 
    # Создание таблицы
    cursor_2db.execute("""CREATE TABLE IF NOT EXISTS employers_tb
                      (employee_number INTEGER PRIMARY KEY AUTOINCREMENT,
                      family_name TEXT,
                      routing_number INTEGER)
                   """)

    cursor_1db.execute("SELECT employee_number, family_name, routing_number FROM employers;")
    result = cursor_1db.fetchall()
    result = list(set(result))
    cursor_2db.executemany("INSERT INTO employers_tb VALUES (?, ?, ?)", result)


    cursor_2db.execute("""CREATE TABLE IF NOT EXISTS dept_tb
                      (routing_number INTEGER PRIMARY KEY AUTOINCREMENT,
                       phone_number INTEGER)
                   """)
    cursor_1db.execute("SELECT routing_number, phone_number FROM employers;")
    result = cursor_1db.fetchall()
    result = list(set(result))
    cursor_2db.executemany("INSERT INTO dept_tb VALUES (?, ?)", result)


    cursor_2db.execute("""CREATE TABLE IF NOT EXISTS projects_tb
                      (project_number INTEGER PRIMARY KEY AUTOINCREMENT,
                       project_name TEXT)
                   """)
    cursor_1db.execute("SELECT project_number, project_name FROM employers;")
    result = cursor_1db.fetchall()
    print(result)

    result = list(set(result))
    print(result)
    cursor_2db.executemany("INSERT INTO projects_tb VALUES (?, ?)", result)

    cursor_2db.execute("""CREATE TABLE IF NOT EXISTS tasks_tb
                      (id_task INTEGER PRIMARY KEY AUTOINCREMENT,
                       employee_number INTEGER,
                       project_number INTEGER,
                       task_number INTEGER)
                   """)
    cursor_1db.execute("SELECT employee_number, project_number, task_number FROM employers;")
    result = cursor_1db.fetchall()
    result = list(set(result))
    cursor_2db.executemany("INSERT INTO tasks_tb VALUES (NULL, ?, ?, ?)", result)


    conn_2db.commit()
    print("Изначальная таблица/n")
    read_tb('initial_db.db', 'employers')
    print("Сотрудники-нормализовання таблица/n")
    read_tb('normalize_db.db', 'employers_tb')
    print("Отделы-нормализовання таблица/n")
    read_tb('normalize_db.db', 'dept_tb')
    print("Проекты-нормализовання таблица/n")
    read_tb('normalize_db.db', 'projects_tb')
    print("Задания-нормализовання таблица/n")
    read_tb('normalize_db.db', 'tasks_tb')

