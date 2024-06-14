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
    

    def student_issued_books(self, STUDENT_ID: int, BOOK_ID: str, ISSUANCE_DATE, RETURN_DATE, LATE_FEES_AMOUNT : int = 0):
        try:
        # Insert values into issued_books table
            self.connection.insert_value_in_table(tablename= 'STUDENT_ISSUED_BOOKS',
                                               STUDENT_ID= STUDENT_ID,
                                               BOOK_ID= BOOK_ID,
                                               ISSUANCE_DATE= ISSUANCE_DATE,
                                               RETURN_DATE= RETURN_DATE,
                                               LATE_FEES_AMOUNT= LATE_FEES_AMOUNT)
            print('Book issuance recorded successfully')
        except Exception as e:
            print('Error recording book issuance:', e)

    
    def book_selection(self,book_id : str, student_id: int):
        """Method to issue book to students and updating the Book_Availability_Status table with updated quantity of books
        and also updating records in Student_Issued_books table. This table contains the records of the students who have issued the books"""
        try:

            today_date = datetime.today().date() #to get the date when student picked up the book
            formatted_today_date = today_date.strftime("%Y-%m-%d")
            return_date = today_date +timedelta(days=7) #to get the return date
            formatted_return_date = return_date.strftime("%Y-%m-%d")

            book_selected=1 #assuming that only 1 book is picked up by student
            query_pick_book=f"UPDATE BOOK_AVAILABILITY_STATUS SET QUANTITY_AVAILABLE = QUANTITY_AVAILABLE - {book_selected}, TRANSACTION_DATE = '{formatted_today_date}'" \
            f"WHERE BOOK_ID = '{book_id}'"
            self.connection.cur.execute(query_pick_book)
            self.connection.conn.commit()
            print('Book picked up successfully')

            self.student_issued_books(STUDENT_ID=student_id,BOOK_ID= book_id,ISSUANCE_DATE=formatted_today_date, RETURN_DATE= formatted_return_date)
        except Exception as e:
            print('Error updating book pickup', e)


    def book_returned(self, student_id: int,book_id : str, late_fees : int = 0):
        """Updating the Book_Availability_Status table quantity column once book is returned by student to library """
        try:

            today_date = datetime.today().date() #to get the date when student picked up the book
            formatted_today_date = today_date.strftime("%Y-%m-%d")
            book_selected=1 #assuming that only 1 book is picked up by student

            query_check_return_date=f"SELECT RETURN_DATE, ISSUANCE_DATE, LATE_FEES_AMOUNT FROM STUDENT_ISSUED_BOOKS WHERE STUDENT_ID={student_id} AND BOOK_ID='{book_id}' "
            result=self.connection.cur.execute(query_check_return_date).fetchone()
            #result=self.connection.cur.fetchone()
            if result:
                return_date, issuance_date, current_late_fees = result
                calculated_late_fees = 0
                if today_date > return_date:
                    days_late = (today_date - return_date).days
                    calculated_late_fees = days_late * 10  # I am assuming $10 per day late fee

                if calculated_late_fees > 0 and late_fees != calculated_late_fees:
                    print(f"Late fees of {calculated_late_fees} must be paid before returning the book")
                    query_update_late_fees=f"UPDATE STUDENT_ISSUED_BOOKS SET LATE_FEES_AMOUNT={calculated_late_fees} WHERE STUDENT_ID={student_id} and BOOK_ID='{book_id}' "
                    self.connection.cur.execute(query_update_late_fees)
                    self.connection.conn.commit()
                    return

            
            query_update_book=f"UPDATE BOOK_AVAILABILITY_STATUS SET QUANTITY_AVAILABLE = QUANTITY_AVAILABLE + {book_selected}, TRANSACTION_DATE = '{formatted_today_date}'" \
            f"WHERE BOOK_ID = '{book_id}'"
            self.connection.cur.execute(query_update_book)
            print('Book returned successfully')

            query_insert_returned = f"""
                    INSERT INTO BOOK_RETURN_HISTORY (
                        STUDENT_ID, BOOK_ID, BOOK_RETURN_DATE, LATE_FEES_AMOUNT
                    ) 
                    VALUES ({student_id},'{book_id}','{formatted_today_date}',{calculated_late_fees}
                    )
                """
            self.connection.cur.execute(query_insert_returned)
            print('Record moved to Book Return History table successfully')

            query_delete_student = f"""
                    DELETE FROM STUDENT_ISSUED_BOOKS 
                    WHERE BOOK_ID = '{book_id}' AND STUDENT_ID = {student_id}
                    """
            self.connection.cur.execute(query_delete_student)
            print('Student deleted from student issued books table successfully')
            self.connection.conn.commit()
            
        except Exception as e:
            print('Error updating book return', e)