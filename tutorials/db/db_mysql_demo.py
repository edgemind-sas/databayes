from databayes.db import DBMySQL

## Creating a configuration object for our database connection
config = dict(
    database="mytestdb",
    host="127.0.0.1",
    port="3306",
    username="root",
    password="my-secret-pw",
)

## Creating an instance of our `DBMySQL` class
db = DBMySQL(name="mytestdb", config=config)

## Connecting to the database
if db.connect():
    print("Successfully connected to the database.")
else:
    print("Failed to connect to the database.")

## Querying data
users = db.query("SELECT * FROM users;")
print("Users:", users)

## Inserting data
new_user = {"name": "Alice Wonderland", "email": "alice@example.com"}
db.insert("users", new_user)
print("Inserted a new user.")

## Updating data
db.update("users", {"email": "newalice@example.com"}, "name = 'Alice Wonderland'")
print("Updated user's email.")

## Deleting data
db.delete("users", "name = 'Alice Wonderland'")
print("Deleted the user.")

## Closing the connection
db.close()
