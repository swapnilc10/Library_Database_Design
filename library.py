from database import Connection
from datetime import datetime, timedelta

class Library:
    def __init__(self, connection):
        self.connection = connection


    def get_book_id_for_book_selection(self,bookname : str):
        """Method to get the  full bookname and its corresponding book id for selecting the book from bookname
        Args:
        bookname (str) : Bookname or part of bookname to get the book id
        """
        query_book="SELECT BOOK_NAME, BOOK_ID FROM BOOK_AVAILABILITY_STATUS WHERE QUANTITY_AVAILABLE>=1 AND BOOK_NAME LIKE '%{}%'".format(bookname)
        self.connection.cur.execute(query_book)
        return self.connection.cur.fetchall()
    
    '''
    def get_book_name(self, book_id: str):
        query_book_name= f"SELECT BOOK_NAME FROM BOOK_AVAILABILITY_STATUS WHERE BOOK_ID ='{book_id}'"
        self.connection.cur.execute(query_book_name)
        return self.connection.cur.fetchone()
    '''
    

    def get_student_rollno(self,student_id: int):
        """Method to get the roll no of the student from student id which is unique
        Args :
        student_id (int) : Id of the student for whom we want to get the roll no. Student Id is unique for each student
        """
        query_student_name= f"SELECT ROLL_NO FROM STUDENT WHERE STUDENT_ID ={student_id}"
        self.connection.cur.execute(query_student_name)
        return self.connection.cur.fetchone()

    
    def book_selection(self,book_id : str, student_id: int):
        """Method to issue book to students and updating the Book_Availability_Status table with updated quantity of books
        and also updating records in Student_Issued_books table. This table contains the records of the students who have issued the books"""
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

            student_rollno= self.get_student_rollno(student_id=student_id)
            ROLL_NO = student_rollno

            
            self.student_issued_books(STUDENT_ID=student_id,BOOK_ID= book_id,ISSUANCE_DATE=formatted_today_date, RETURN_DATE= formatted_return_date)
        except Exception as e:
            print('Error updating book pickup', e)


    def student_issued_books(self, STUDENT_ID: int, BOOK_ID: str, ISSUANCE_DATE, RETURN_DATE):
        try:
        # Insert values into issued_books table
            self.connection.insert_value_in_table(tablename= 'STUDENT_ISSUED_BOOKS',
                                               STUDENT_ID= STUDENT_ID,
                                               BOOK_ID= BOOK_ID,
                                               ISSUANCE_DATE= ISSUANCE_DATE,
                                               RETURN_DATE= RETURN_DATE)
            print('Book issuance recorded successfully')
        except Exception as e:
            print('Error recording book issuance:', e)


    def book_returned(self, book_id : str):
        """Updating the Book_Availability_Status table quantity column once book is returned by student to library """
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