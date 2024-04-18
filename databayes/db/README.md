# Databayes DB package

## DBMySQL

In this tutorial, we'll go through setting up a simple MySQL database using Docker and interacting with it using a Python wrapper class `DBMySQL`. We'll start by setting up the MySQL database with Docker, populate it with some fake data, and then use the `DBMySQL` class to execute basic database operations.

### Step 1: Setting Up MySQL with Docker

First, we need Docker installed on your machine. If you haven't installed Docker yet, follow the instructions on the [official Docker website](https://docs.docker.com/get-docker/).

Once Docker is installed, run the following command to start a MySQL instance:

```bash
docker run --name mysql-demo -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=mytestdb -p 3306:3306 -d mysql:latest
```

This command will:
- Start a new Docker container named `mysql-demo`.
- Set the root password to `my-secret-pw`.
- Create a new database named `mytestdb`.
- Forward the local port `3306` to the container's port `3306`.
- Use the latest MySQL image.


### Step 2: Populating the Database with Fake Data

Let's create a simple table and insert some fake data. First, access the MySQL console in your container:

```bash
docker exec -it mysql-demo mysql -uroot -pmy-secret-pw mytestdb
```

Next, create a table and populate it:

```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);

INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com'), ('Jane Doe', 'jane@example.com');
```

### Step 3: Interacting with MySQL Using `DBMySQL`

Ensure you have the necessary Python package installed:

```bash
pip install mysql-connector-python
```

Copy the `DBMySQL` class implementation from the previous response into a Python file (e.g., `db_mysql.py`).

Now, let's write a Python script `mysql_demo.py` to demonstrate using the `DBMySQL` class:

```python
from db_mysql import DBMySQL, DMBSConfigBase

## Creating a configuration object for our database connection
config = DMBSConfigBase(
    database="mytestdb",
    host="127.0.0.1",
    port="3306",
    username="root",
    password="my-secret-pw"
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
```

Run the script:

```bash
python mysql_demo.py
```

You should see output indicating that the connection was successful and the operations (insert, update, delete) were performed.

### Conclusion

You've successfully set up a MySQL database in a Docker container, populated it with fake data, and used a Python class to interact with it. This setup is useful for development and testing, as it allows for quick database provisioning and isolation. Feel free to extend the `DBMySQL` class with more functionality as needed.

This tutorial provided a basic introduction. As you become more comfortable, explore more complex queries, transactions, and database schema designs to enhance your applications.
