from database import Connection
from library import Library
from datetime import datetime, timedelta
import pandas as pd

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
        """This method is going to return all the students who have books with them right now"""

        query_issued_books= f"SELECT * FROM STUDENT_ISSUED_BOOKS"
        result= self.connection.cur.execute(query_issued_books).fetchall()
        columns=[col[0] for col in self.connection.cur.description]
        df=pd.DataFrame(result,columns=columns)

        today_date = datetime.today().date()
        for index, row in df.iterrows():
            return_date = row['RETURN_DATE']
            if today_date > return_date:
                days_late = (today_date - return_date).days
                late_fees = days_late * 10  # Assuming $10 per day late fee
                df.at[index, 'LATE_FEES_AMOUNT'] = late_fees
            else:
                df.at[index, 'LATE_FEES_AMOUNT'] = 0
        return df
    
  
    def student_issued_books(self,student_id: int):
        """This method will return record only for the student for whom we are going to provide the student id"""

        query_issued_books = f"SELECT * FROM STUDENT_ISSUED_BOOKS WHERE STUDENT_ID = {student_id}"
        result=self.connection.cur.execute(query_issued_books).fetchone()
        if result:
            columns=[col[0] for col in self.connection.cur.description]
            df=pd.DataFrame(result,columns=columns)
            return df
        else:
            return None

    
    def book_extension(self,student_id: int,days: int = 7):
        """Method to provide extension to student if they want to keep books for some longer duration
        Book duration can only be extended if student is trying to extend book before return date
        Args :
        student_id (int) : Id of the student who wants to extend the book duration
        days (int) : Number of days for which student wants to extend the book,by default it is 7 days
        """

        try:
            issued_book = self.student_issued_books(student_id)
            if issued_book:
                today_date = datetime.today().date() #to get today's date
                return_date= issued_book.at[0,'RETURN_DATE']

                if today_date < return_date :
                    new_return_date = today_date + timedelta(days=days) #to get the extended return date
                    formatted_return_date = new_return_date.strftime("%Y-%m-%d")

                    query_update_book=f"UPDATE STUDENT_ISSUED_BOOKS SET RETURN_DATE = '{formatted_return_date}'" \
                    f"WHERE STUDENT_ID = '{student_id}'"
                    self.connection.cur.execute(query_update_book)
                    self.connection.conn.commit()
                    print('Return date updated successfully')
                else:
                    print('Cannot extend the book duration as return date is already passed',return_date)
            else:
                print(f'No issued book found for student id{student_id}')
        except Exception as e:
            print('Error extending return date', e)

