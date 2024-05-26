from database import Connection
from library import Library
from datetime import datetime, timedelta

class Student:

    def __init__(self,connection):
        self.connection=connection

    def student_creation(self,ROLL_NO: int,FIRST_NAME: str,LAST_NAME: str,STANDARD: int,STANDARD_YEAR: str):
        """Method to create student record in Student table"""

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
        
    def get_all_student_issued_books(self):
        query_issued_books= f"SELECT * FROM STUDENT_ISSUED_BOOKS"
        self.connection.cur.execute(query_issued_books)
        return self.connection.cur.fetchall()
    
    def student_issued_books(self,student_id: int):
        query_issued_books = f"SELECT * FROM STUDENT_ISSUED_BOOKS WHERE STUDENT_ID = {student_id}"
        self.connection.cur.execute(query_issued_books)
        return self.connection.cur.fetchone()

    
    def book_extension(self,student_id: int,days: int):
        """Method to provide extension to student if they want to keep books for some longer duration
        Args :
        student_id (int) : Id of the student who wants to extend the book duration
        days (int) : Number of days for which student wants to extend the book
        """
        try:
            issued_book = self.student_issued_books(student_id)
            if issued_book:
                current_return_date_str = issued_book['RETURN_DATE']
                current_return_date = datetime.strptime(current_return_date_str, '%Y-%m-%d')

                new_return_date = current_return_date +timedelta(days=days) #to get the extended return date
                formatted_return_date = new_return_date.strftime("%Y-%m-%d")

                query_update_book=f"UPDATE STUDENT_ISSUED_BOOKS SET RETURN_DATE = '{formatted_return_date}'" \
            f"WHERE STUDENT_ID = '{student_id}'"
                self.connection.cur.execute(query_update_book)
                self.connection.conn.commit()
                print('Return date updated successfully')
            else:
                print(f'No issued book found for student id{student_id}')
        except Exception as e:
            print('Error extending return date', e)

