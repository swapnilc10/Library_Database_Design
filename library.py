from database import Connection
from datetime import datetime, timedelta

class Library:
    def __init__(self, connection):
        self.connection = connection


    def get_book_id_for_book_selection(self,bookname : str):
        query_book="SELECT BOOK_NAME, BOOK_ID FROM BOOK_AVAILABILITY_STATUS WHERE QUANTITY_AVAILABLE>=1 AND BOOK_NAME LIKE '%{}%'".format(bookname)
        self.connection.cur.execute(query_book)
        return self.connection.cur.fetchall()
    

    def get_book_name(self, book_id: str):
        query_book_name= f"SELECT BOOK_NAME FROM BOOK_AVAILABILITY_STATUS WHERE BOOK_ID ='{book_id}'"
        self.connection.cur.execute(query_book_name)
        return self.connection.cur.fetchone()
    

    def get_student_name(self,student_id: int):
        query_student_name= f"SELECT FIRST_NAME, LAST_NAME FROM STUDENT WHERE STUDENT_ID ={student_id}"
        self.connection.cur.execute(query_student_name)
        return self.connection.cur.fetchone()

    
    def book_selection(self,book_id : str, student_id: int):
        try:

            today_date = datetime.today().date() #to get the date when student picked up the book
            formatted_today_date = today_date.strftime("%Y-%m-%d")
            return_date = today_date +timedelta(days=14) #to get the return date
            formatted_return_date = return_date.strftime("%Y-%m-%d")

            book_selected=1 #assuming that only 1 book is picked up by student
            query_pick_book=f"UPDATE BOOK_AVAILABILITY_STATUS SET QUANTITY_AVAILABLE = QUANTITY_AVAILABLE - {book_selected}, TRANSACTION_DATE = '{formatted_today_date}'" \
            f"WHERE BOOK_ID = '{book_id}'"
            self.connection.cur.execute(query_pick_book)
            self.connection.conn.commit()
            print('Book picked up successfully')

            student_name= self.get_student_name(student_id=student_id)
            FIRST_NAME, LAST_NAME = student_name

            book_name= self.get_book_name(book_id=book_id)
            book_name = book_name.replace("'", "''")
            BOOK_NAME =book_name
            
            self.student_issued_books(STUDENT_ID=student_id,FIRST_NAME=FIRST_NAME, LAST_NAME= LAST_NAME,BOOK_NAME= BOOK_NAME,BOOK_ID= book_id,ISSUANCE_DATE=formatted_today_date, RETURN_DATE= formatted_return_date)
        except Exception as e:
            print('Error updating book pickup', e)


    def student_issued_books(self, STUDENT_ID: int, FIRST_NAME: str, LAST_NAME: str, BOOK_NAME: str, BOOK_ID: str, ISSUANCE_DATE, RETURN_DATE):
        try:
        # Insert values into issued_books table
            self.connection.insert_value_in_table(tablename= 'STUDENT_ISSUED_BOOKS',
                                               STUDENT_ID= STUDENT_ID,
                                               FIRST_NAME= FIRST_NAME,
                                               LAST_NAME= LAST_NAME,
                                               BOOK_NAME= BOOK_NAME,
                                               BOOK_ID= BOOK_ID,
                                               ISSUANCE_DATE= ISSUANCE_DATE,
                                               RETURN_DATE= RETURN_DATE)
            print('Book issuance recorded successfully')
        except Exception as e:
            print('Error recording book issuance:', e)


    def book_returned(self, book_id : str):
        try:

            today_date = datetime.today().date() #to get the date when student picked up the book
            formatted_today_date = today_date.strftime("%Y-%m-%d")
            book_selected=1 #assuming that only 1 book is picked up by student
            
            query_pick_book=f"UPDATE BOOK_AVAILABILITY_STATUS SET QUANTITY_AVAILABLE = QUANTITY_AVAILABLE + {book_selected}, TRANSACTION_DATE = '{formatted_today_date}'" \
            f"WHERE BOOK_ID = {book_id}"
            self.connection.cur.execute(query_pick_book)
            self.connection.conn.commit()
            print('Book picked up successfully')
        except Exception as e:
            print('Error updating book pickup', e)