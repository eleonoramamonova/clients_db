from pprint import pprint

import psycopg2

def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(30),
        email VARCHAR(254)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)

def delete_db(cur):
    cur.execute("""
        DROP TABLE clients, phonenumbers CASCADE;
        """)
def add_phone(cur, client_id, phones):
    cur.execute("""
            INSERT INTO phonenumbers(number, client_id)
            VALUES (%s, %s)
            """, (phones, client_id))
    return client_id

def add_client(cur, first_name, last_name, email, phones=None):
    cur.execute("""
            INSERT INTO clients(first_name, last_name, email)
            VALUES (%s, %s, %s)
            """, (first_name, last_name, email))
    cur.execute("""
            SELECT id from clients
            ORDER BY id DESC
            LIMIT 1
            """)
    id = cur.fetchone()[0]
    if phones is None:
        return id
    else:
        add_phone(cur, id, phones)
        return id

def change_client(cur, id, first_name=None, last_name=None, email=None):
    cur.execute("""
            SELECT * from clients
            WHERE id = %s
            """, (id,))
    info = cur.fetchone()
    if first_name is None:
        first_name = info[1]
    if last_name is None:
        last_name = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
            UPDATE clients
            SET first_name = %s, last_name = %s, email = %s
            where id = %s
            """, (first_name, last_name, email, id))
    return id

def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM phonenumbers 
        WHERE number = %s
        """, (number, ))
    return number


def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id, ))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id

def find_client(cur, first_name=None, last_name=None, email=None, phones=None):
    if first_name is None:
        first_name = '%'
    else:
        first_name = '%' + first_name + '%'
    if last_name is None:
        last_name = '%'
    else:
        last_name = '%' + last_name + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if phones is None:
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s
            """, (first_name, last_name, email))
    else:
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.first_name LIKE %s AND c.last_name LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (first_name, last_name, email, phones))
    return cur.fetchall()

if __name__ == '__main__':
    with psycopg2.connect(database="clients_db", user="postgres",
                          password="glamur321") as conn:
        with conn.cursor() as curs:
            # Удаление таблиц перед запуском
            delete_db(curs)
            # 1. Cоздание таблиц
            create_db(curs)
            print("БД создана")
            # 2. Добавляем 5 клиентов
            print("Добавлен клиент id: ",
                  add_client(curs, "Михаил", "Кораблев", "qwerty54@gmail.com"))
            print("Добавлен клиент id: ",
                  add_client(curs, "Егор", "Демидов",
                                "vmir43@mail.ru", 79045761432))
            print("Добавлен клиент id: ",
                  add_client(curs, "Леонид", "Иванов",
                                "net3@outlook.com", 79058765444))
            print("Добавлен клиент id: ",
                  add_client(curs, "Илья", "Давыдов",
                                "ksr4@mail.ru", 79066871362))
            print("Добавлен клиент id: ",
                  add_client(curs, "Андрей", "Жилин",
                                "jili6@outlook.com"))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 3. Добавляем клиенту номер телефона(одному первый, одному второй)
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 2, 79097682312))
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 1, 79623347653))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 4. Изменим данные клиента
            print("Изменены данные клиента id: ",
                  change_client(curs, 4, "Иван", None, '123@outlook.com'))
            # 5. Удаляем клиенту номер телефона
            print("Телефон удалён c номером: ",
                  delete_phone(curs, '79058765444'))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 6. Удалим клиента номер 2
            print("Клиент удалён с id: ",
                  delete_client(curs, 2))
            curs.execute("""
                            SELECT c.id, c.first_name, c.last_name, c.email, p.number FROM clients c
                            LEFT JOIN phonenumbers p ON c.id = p.client_id
                            ORDER by c.id
                            """)
            pprint(curs.fetchall())
            # 7. Найдём клиента
            print('Найденный клиент по имени:')
            pprint(find_client(curs, 'Леонид'))

            print('Найденный клиент по email:')
            pprint(find_client(curs, None, None, 'qwerty54@gmail.com'))

            print('Найденный клиент по имени, фамилии и email:')
            pprint(find_client(curs, 'Андрей', 'Жилин',
                               'jili6@outlook.com'))

            print('Найденный клиент по имени, фамилии, телефону и email:')
            pprint(find_client(curs, 'Иван', 'Давыдов',
                               '123@outlook.com', '79066871362'))

            print('Найденный клиент по имени, фамилии, телефону:')
            pprint(find_client(curs, None, None, None, '79066871362'))