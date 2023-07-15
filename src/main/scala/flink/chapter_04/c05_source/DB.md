# MyQql operation

1. connect to mysql

```shell
mysql -u root -p
```

then enter password.

2. show database;

```shell
 show databases;
```

3. create database

```shell
create database student_db;
```

4. use database;

```shell
use student_db
```

5. create table

```
 create table student(id int primary key, name varchar(100), age int));
```

6. insert some data

```shell
INSERT INTO student (id, name, age)
VALUES (1, 'Alice', 20), (2, 'Bob', 22), (3, 'Charlie', 25);
```

7. exit

```shell
exit 
```