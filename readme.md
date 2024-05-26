In this project I have created a Library book management system

Database.py 

In this python file I have created a Snowflake python connection which we are going to use to connect to Snowflake
Some of the important methods are
create_table method :- To create a new table in Snowflake
insert_value_in_table :- To add values in the table
add_column_to_table :- To add new column to a table
upload_csv_to_snowflake :- Uploading pandas dataframe to Snowflake table


Library.py

In this python file I have methods related to library functioning
Some of the important methods are
get_book_id_for_book_selection :- This method is used to get the book_id from the bookname or part of bookname
book_selection :- book to be issued to student, and will create a new table that will have the records for book issuance to students
book_returned :- Updating the records once book is returned to library by student


student.py

In this python file I have methods related to students 

This file is still work in progress, and I will create more methods in it