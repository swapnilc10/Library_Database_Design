import snowflake.connector
#from dotenv import load_dotenv
import uuid

#load_dotenv()

class Connection:

    def __init__(self, user, password, account, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema


    def connect(self):
        try:
            self.conn=snowflake.connector.connect(
            user = self.user,
            password = self.password ,
            account = self.account,
            database = self.database,
            schema = self.schema
        )
            print('Connection is successful')
        except BaseException as e:
            print('There is something wrong with the input parameters', e.__class__.__name__)


    def create_table(self,tablename,schema):
        query_create='create or replace table {} ({})'.format(tablename,schema)
        cur=self.conn.cursor()
        cur.execute(query_create)
        self.conn.commit()
        print('Table is created')


    def insert_value_in_table(self,tablename,**kwargs):
        """
        Insert values into a specified table.

        Args:
            tablename (str): Name of the table to insert values into.
            **kwargs: Keyword arguments representing column names and their values.
                      For example, insert_value('student_table', rollno=1, firstname='John', lastname='Doe', standard=10)
        """
        columns = ', '.join(kwargs.keys())
        values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in kwargs.values()])
        query_insert = f"INSERT INTO {tablename} ({columns}) VALUES ({values})"
        cur = self.conn.cursor()
        cur.execute(query_insert)
        self.conn.commit()
        print('Values are inserted')

    
    def generate_uuid(self):
        return str(uuid.uuid4())


    def load_dataframe_in_table(self, dataframe, table_name, include_uuid=False):
        if include_uuid:
            dataframe['id'] = dataframe.apply(lambda row: self.generate_uuid(), axis=1)  # Add UUID column
        dataframe.to_sql(table_name, con=self.conn, schema=self.schema, if_exists='replace', index=False)


    def get_row_count(self, table):
        query = f"SELECT COUNT(*) FROM {table}"
        self.cur.execute(query)
        return self.cur.fetchone()[0]


    def create_backup_table(self, original_table, backup_table):
        query = f"CREATE TABLE {backup_table} LIKE {original_table}"
        self.cur.execute(query)

        # Copy data from original table to backup table
        query = f"INSERT INTO {backup_table} SELECT * FROM {original_table}"
        self.cur.execute(query)


    def restore_table(self, original_table, backup_table):
        # Truncate the original table to remove existing data
        self.cur.execute(f"TRUNCATE TABLE {original_table}")

        # Copy data from backup table back into original table
        self.cur.execute(f"INSERT INTO {original_table} SELECT * FROM {backup_table}")    

    def close(self):
        self.cur.close()
        self.conn.close()