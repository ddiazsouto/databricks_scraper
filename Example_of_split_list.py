from datetime import datetime


notebooks = [['Scrap it Idle', 'Mon, Mar 13, 2023, 12:14:50 GMT', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Scrap it'],
             ['Test Idle', 'Mon, Mar 13, 2023, 13:32:03 GMT', 'by ddiazsouto@gmail.com', '/Users/ddiazsouto@gmail.com/Test']]


def extract_date(date_string):
    out = date_string.split(", ")[1:]

    month, day = out[0].split(" ")
    year = out[1]
    time = out[-1].split(" ")[0]

    date = f"{year}-{month}-{day}"

    output_date = datetime.strptime(date, '%Y-%b-%d').date()
    print(time)
    return str(output_date) + ': ' + time


def extract_user(user_string: str):
    return user_string.split(" ")[-1]


information = {}
for notebook in notebooks:
    date = extract_date(notebook[1])
    user = extract_user(notebook[2])
    information[date] = user

breakpoint() 

