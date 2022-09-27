import psycopg2
from pprint import pprint

conn = psycopg2.connect(database='personalsdb', user='postgres', password='Ingrad2022')


def create_tables(conn):
    with conn.cursor() as cur:
        # cur.execute("""
        # DROP TABLE personaldata;
        # DROP TABLE phones;
        # """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personaldata(
                personal_id SERIAL PRIMARY KEY,
                firstname VARCHAR(100) NOT NULL,
                lastname VARCHAR(100) NOT NULL,
                email VARCHAR(150)
                );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phones(
                phone_id SERIAL PRIMARY KEY,
                number BIGINT NOT NULL
                );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personal_phone (
                personal_id INTEGER references personaldata(personal_id),
                phone_id INTEGER references phones(phone_id),
                CONSTRAINT per_phone_id PRIMARY key (personal_id, phone_id)
                );
            """)
        conn.commit()
    conn.close()


def add_new_person(conn, firstname, lastname, email, phones=None):
    if phones:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO personaldata (firstname, lastname, email) VALUES
                    (%s, %s, %s)
                    RETURNING personal_id; 
                """, (firstname, lastname, email))
            pd_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO phones (number) VALUES
                    (%s)
                    RETURNING phone_id;
                """, (phones,))
            ph_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO personal_phone (personal_id, phone_id) VALUES
                    (%s, %s);
                """, (pd_id, ph_id))
            conn.commit()
        conn.close()
    else:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO personaldata (firstname, lastname, email) VALUES
                    (%s, %s, %s); 
                """, (firstname, lastname, email))
            conn.commit()
        conn.close()


def add_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phones (number) VALUES
                (%s)
                RETURNING phone_id;
            """, (number,))
        ph_id = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO personal_phone (personal_id, phone_id) VALUES
                (%s, %s);
            """, (client_id, ph_id))
        conn.commit()
    conn.close()


# если не введен какой то аргумент, ставится NULL
def edit_client(conn, client_id, firstname=None, lastname=None, email=None, phones=None):
    if phones:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE personaldata 
                SET firstname  = %s,
                    lastname  = %s,
                    email = %s
                WHERE  personal_id = %s;
                """, (firstname, lastname, email, client_id))
            cur.execute("""
                select * from personal_phone
                where personal_id = %s;
                """, (client_id,))
            ph_id = cur.fetchone()[1]
            cur.execute("""
                UPDATE phones 
                SET number  = %s
                WHERE  phone_id = %s;
                """, (phones, ph_id))
            conn.commit()
        conn.close()
    else:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE personaldata 
                SET firstname  = %s,
                    lastname  = %s,
                    email = %s
                WHERE  personal_id = %s;
                """, (firstname, lastname, email, client_id))
            conn.commit()
        conn.close()


def del_phone(conn, client_id, number):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT phone_id from phones 
            where number = %s;
            """, (number,))
        ph_id = cur.fetchone()[0]
        cur.execute("""
            delete from personal_phone 
            where personal_id = %s and phone_id = %s;
            """, (client_id, ph_id))
        cur.execute("""
            delete from phones
            where number = %s;
            """, (number,))
        conn.commit()
    conn.close()


# ошибка если введен не верный client_id
def del_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            select firstname, lastname, email, number from personaldata p
            left join personal_phone pp on p.personal_id = pp.personal_id 
            left join phones p2 on pp.phone_id = p2.phone_id
            where p.personal_id  = %s;
            """, (client_id,))
        obj = cur.fetchone()
        if len(obj) == 4:
            cur.execute("""
                SELECT phone_id from personal_phone
                where personal_id = %s;
                """, (client_id,))
            ph_id = cur.fetchone()[0]
            cur.execute("""
                delete from personal_phone
                where personal_id = %s;
                """, (client_id,))
            cur.execute("""
                delete from phones
                where phone_id = %s;
                """, (ph_id,))
            cur.execute("""
                delete from personaldata
                where personal_id = %s;
                """, (client_id,))
            conn.commit()
        else:
            cur.execute("""
                delete from personaldata
                where personal_id = %s;
                """, (client_id,))
            conn.commit()
    conn.close()


def find_client(conn, firstname=None, lastname=None, email=None, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            select firstname, lastname, email, number from personaldata p
            left join personal_phone pp on p.personal_id = pp.personal_id 
            left join phones p2 on pp.phone_id = p2.phone_id
            where firstname = %s or lastname = %s or email = %s or number = %s;
            """, (firstname, lastname, email, phones))
        pprint(cur.fetchall())
    conn.close()

if __name__ == '__main__':
    # create_tables(conn)
    # add_new_person(conn, '111', '111', '111@mail.ru', 11111111)
    # add_new_person(conn, '222', '222', '222@mail.ru', 22222222)
    # add_new_person(conn, '333', '333', '333@mail.ru', 33333333)
    # add_new_person(conn, '444', '444', '444@mail.ru', 44444444)
    # add_phone(conn, 2, 23232323232)
    # edit_client(conn, 3, '444', '444', '444@inbox.ru')
    # del_phone(conn, 9, 22222222)
    # find_client(conn, None, None, None, 11111111)
    del_client(conn, 4)
