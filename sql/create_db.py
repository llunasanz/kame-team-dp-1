import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="kamehameha"
)

mycursor = mydb.cursor()

with open('sql/edem.sql') as f:
    test_var = list(f)
# sql_query = ''.join(''.join(test_var).split('\n'))
sql_query = ''.join(test_var).split('\n')
while True:
    try:
        for x in sql_query:
            mycursor.execute(x)
    except:
        pass
    else:
        break


print("[create_db.py] Database created!")

# mycursor.execute(sql_query, multi=True)