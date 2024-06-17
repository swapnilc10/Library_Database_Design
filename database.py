import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime


class Connection:

    
    def __init__(self, user: str, password: str, account: str, database: str, schema: str):
    

        try:
            self.conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                database=database,
                schema=schema
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


    def upload_csv_to_snowflake(self, dataframe, tablename: str):
        """Method to upload csv file to a table
        
        Args:
        dataframe : Name of the pandas dataframe that you want to add to snowflake
        tablename (str) : Name of the table to which we want to add the dataframe. Table should be present in Snowflake,
        no new table is going to be created
        """

        try:
            write_pandas(df= dataframe, table_name= tablename,conn=self.conn, auto_create_table= False)
            self.conn.commit()
            print("CSV file uploaded to Snowflake successfully.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()


    def get_row_count(self, tablename : str):
        """Method to get the total number of rows of the table
        Args:
        tablename (str) : Name of the table
        """
        query = f"SELECT COUNT(*) FROM {tablename}"
        self.cur.execute(query)
        return self.cur.fetchone()[0] # type: ignore


    def create_backup_table(self, original_table : str, backup_table : str):
        """Method to create the backup
        Args:
        original_table (str) : Name of the original table for which we want to create backup
        backup_table (str) : Name of the backup table
        """
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


    def create_book_availability_status_table(self):
        try:
            # Get today's date
            today_date = datetime.date.today()

            query_select = "SELECT BOOK_NAME, BOOK_QUANTITY, BOOK_ID FROM LIBRARY_BOOKS"

            self.cur.execute(query_select)
            records=self.cur.fetchall()

            for record in records:
                book_name, book_quantity, book_id = record

                book_name = book_name.replace("'", "''")

            # Insert a new record into the TRANSACTION_DETAILS table
                query_insert = f"INSERT INTO BOOK_AVAILABILITY_STATUS (BOOK_ID, BOOK_NAME, TRANSACTION_DATE, QUANTITY_AVAILABLE) " \
                           f"VALUES ('{book_id}', '{book_name}','{today_date}', {book_quantity})"

                self.cur.execute(query_insert)
            self.conn.commit()
            print('Initial data replicated successfully.')
        except Exception as e:
            print('Error replicating initial data:', e)


    def close(self):
        self.cur.close()
        self.conn.close()