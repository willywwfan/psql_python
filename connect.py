import psycopg2
from datetime import datetime

class PostgresBaseManager:

    def __init__(self):

        self.database = 'd9r8va4pq94vp5'
        self.user = 'vnnvynpuzipegu'
        self.password = 'e17d5a6ec332f03123b57b0fddd18a7bc5dcbda5908853191d84f293bab4bd3c'
        self.host = 'ec2-52-203-118-49.compute-1.amazonaws.com'
        self.port = '5432'
        self.conn = self.connectServerPostgresDb()

    def connectServerPostgresDb(self):
        """
        :return: 連接 Heroku Postgres SQL 認證用
        """
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            sslmode="require")
        return conn

    def closePostgresConnection(self):
        """
        :return: 關閉資料庫連線使用
        """
        self.conn.close()

    def runServerPostgresDb(self):
        """
        :return: 測試是否可以連線到 Heroku Postgres SQL
        """
        cur = self.conn.cursor()
        cur.execute('SELECT VERSION()')
        results = cur.fetchall()
        print("Database version : {0} ".format(results))
        self.conn.commit()

        cur.execute("SELECT * FROM messege;")
        rows = cur.fetchall()
        # Print all rows
        for row in rows:
            print("Data row = (%s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2])))

    def delete(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS messege")
        self.conn.commit()
        print("drop finnish!")

    def listtable(self):
        cur = self.conn.cursor()
        slect_query = '''SELECT tablename FROM pg_tables
            WHERE tablename NOT LIKE 'pg%'
            AND tablename NOT LIKE 'sql_%'
            ORDER BY tablename;
            '''
        cur.execute(slect_query)
        self.conn.commit()
        select = cur.fetchall()
        print("All table:", select)

    def createtable(self):
        cur = self.conn.cursor()
        create_table_query = '''CREATE TABLE accounts_table(
           record_no serial PRIMARY KEY,
           owner_name VARCHAR (50) NOT NULL,
           text VARCHAR (50) NOT NULL,
           amount INTEGER NOT NULL,
           date DATE NOT NULL
        );'''
        cur.execute(create_table_query)
        self.conn.commit()
        print("create finish!")

    def select(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM accounts_table WHERE DATE_PART('week', date) = DATE_PART('week', NOW());")
        rows = cur.fetchall()
        self.selected = ""
        for row in rows:
            datas = ""
            for data in row:
                datas = datas + str(data) + ", "
            self.selected += datas + "\n\t\n"
        # self.selected -= "\n\t\n"

    def alter(self):
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE accounts_table ALTER COLUMN date TYPE timestamp USING date::timestamp;")
        self.conn.commit()

    def insert(self):
        cur = self.conn.cursor()
        date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        cur.execute("INSERT INTO accounts_table (owner_name, text, amount, date) VALUES (%s, %s, %s, %s);", ("Willy", "test1", -990, date))
        date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        cur.execute("INSERT INTO accounts_table (owner_name, text, amount, date) VALUES (%s, %s, %s, %s);", ("Willy", "test2", -2990, date))
        date = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        cur.execute("INSERT INTO accounts_table (owner_name, text, amount, date) VALUES (%s, %s, %s, %s);", ("Willy", "test3", 54000, date))
        print("Inserted finish!")
        self.conn.commit()

        select = "SELECT MAX(record_no) FROM accounts_table"
        cur.execute(select)
        max_record_no = cur.fetchall()[0][0]
        print(max_record_no)

        limit = "DELETE FROM accounts_table WHERE record_no < " + str(max_record_no-19) + " RETURNING *;"
        cur.execute(limit)
        self.conn.commit()
        deleted = cur.fetchall()
        print("deleted:")
        for row in deleted:
            datas = ""
            for data in row:
                datas = datas + str(data) + ", "
            print("Data row = " + datas )

if __name__ == '__main__':
    postgres_manager = PostgresBaseManager()

    # postgres_manager.runServerPostgresDb()
    # postgres_manager.listtable()
    # postgres_manager.delete()
    # postgres_manager.createtable()
    # postgres_manager.select()
    # postgres_manager.insert()
    # postgres_manager.alter()
    postgres_manager.select()
    print(postgres_manager.selected)

    postgres_manager.closePostgresConnection()
