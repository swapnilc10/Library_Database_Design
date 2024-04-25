import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import uuid


class Connection:

    def __init__(self, user, password, account, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.database = database
        self.schema = schema
        self.connect()  #Call the connect method to establish the connection


    def connect(self):
        try:
            self.conn=snowflake.connector.connect(
            user = self.user,
            password = self.password ,
            account = self.account,
            database = self.database,
            schema = self.schema
        )
            self.cur = self.conn.cursor()
            print('Connection is successful')
        except BaseException as e:
            print('There is something wrong with the input parameters', e.__class__.__name__)


    def create_table(self,tablename : str,schema : str):

        """ Creates a table with tablename and schema

        Parameters
        tablename : string
        schema : sting, with columnname and columntype. Ex: 'ROLL_NO INT, FIRST_NAME VARCHAR, YEAR DATE, IF_TRUE BOOLEAN'
        """
        query_create='create or replace table {} ({})'.format(tablename,schema)
        cur=self.conn.cursor()
        cur.execute(query_create)
        self.conn.commit()
        print(f'Table {tablename} is created')


    def insert_value_in_table(self,tablename : str,**kwargs):
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
        print(f'Values are inserted in {tablename}')


    def add_columns_to_table(self, tablename : str, columns : dict):
        """
        Add columns to an existing table.

        Args:
            table_name (str): Name of the table to add columns to.
            columns (dict): Dictionary containing column names as keys and their data types as values.
                            Example: {'column1': 'VARCHAR(255)', 'column2': 'INT'}
        """
        try:
            for column_name, data_type in columns.items():
                query_alter = f"ALTER TABLE {tablename} ADD COLUMN {column_name} {data_type}"
                cur = self.conn.cursor()
                cur.execute(query_alter)
                self.conn.commit()
                print(f"Column '{column_name}' added to table '{tablename}' with data type '{data_type}'")
        except Exception as e:
            print('Error:', e)

    
    def generate_uuid(self):
        return str(uuid.uuid4())


    def load_dataframe_in_table(self, dataframe , table_name : str, include_uuid=False):
        if include_uuid:
            dataframe['id'] = [self.generate_uuid() for _ in range(len(dataframe))]
            #dataframe.apply(lambda row: self.generate_uuid(), axis=1)  # Add UUID column
        write_pandas(dataframe, table_name, conn=self.conn)
        self.conn.commit()
        #dataframe.to_sql(table_name, con=self.conn, schema=self.schema, if_exists='replace', index=False)


    def get_row_count(self, tablename : str):
        query = f"SELECT COUNT(*) FROM {tablename}"
        self.cur.execute(query)
        return self.cur.fetchone()[0]


    def create_backup_table(self, original_table : str, backup_table : str):
        query = f"CREATE TABLE {backup_table} LIKE {original_table}"
        self.cur.execute(query)

        # Copy data from original table to backup table
        query = f"INSERT INTO {backup_table} SELECT * FROM {original_table}"
        self.cur.execute(query)


    def restore_table(self, original_table : str, backup_table : str):
        # Truncate the original table to remove existing data
        self.cur.execute(f"TRUNCATE TABLE {original_table}")

        # Copy data from backup table back into original table
        self.cur.execute(f"INSERT INTO {original_table} SELECT * FROM {backup_table}")    

    def close(self):
        self.cur.close()
        self.conn.close()