from database import Connection
from library import Library

class Student:

    def __init__(self,connection):
        self.connection=connection

    def student_creation(self,ROLL_NO,FIRST_NAME,LAST_NAME,STANDARD,STANDARD_YEAR):
        self.ROLL_NO= ROLL_NO
        self.FIRST_NAME= FIRST_NAME
        self.LAST_NAME= LAST_NAME
        self.STANDARD= STANDARD
        self.STANDARD_YEAR = STANDARD_YEAR
        self.connection.insert_value_in_table(tablename = 'STUDENT',
                                         ROLL_NO= ROLL_NO,
                                         FIRST_NAME= FIRST_NAME,
                                         LAST_NAME= LAST_NAME,
                                         STANDARD= STANDARD,
                                         STANDARD_YEAR= STANDARD_YEAR)
        

