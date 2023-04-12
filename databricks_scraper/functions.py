from datetime import datetime


data = 'Scrap it Idle\nFri, Mar 17, 2023, 16:18:22 GMT\nby ddiazsouto@gmail.com\n/Users/ddiazsouto@gmail.com/Scrap it\nTest Idle\nFri, Mar 17, 2023, 16:18:11 GMT\nby ddiazsouto@gmail.com\n/Users/ddiazsouto@gmail.com/Test\nFour Idle\nFri, Mar 17, 2023, 16:18:30 GMT\nby ddiazsouto@gmail.com\n/Users/ddiazsouto@gmail.com/Contains/Four'


def extract_date(table_array):
    select_table_row_with_date_info = table_array[1]
    date_time_split = select_table_row_with_date_info.split(", ")[1:]
    month, day = date_time_split[0].split(" ")
    year = date_time_split[1]
    time = date_time_split[-1].split(" ")[0]

    output_date = f"{year}-{month}-{day}"
    
    return str(output_date) + ': ' + time


def extract_user(table_array: str):
    select_table_row_with_user_info = table_array[2]
    return select_table_row_with_user_info.split(" ")[-1]


def build_array(stringing, number_of_columns=4):
    split_table_cells = stringing.split("\n")
    grouped_notebooks = [split_table_cells[N:N+4] for N in range(0, len(split_table_cells), number_of_columns)]
    return grouped_notebooks