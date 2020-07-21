CREATE DATABASE DataWarehouse;
USE DataWarehouse;
CREATE TABLE STUDENT(
	stt int(10) NOT NULL  AUTO_INCREMENT PRIMARY KEY,
    id_student int (10) NOT NULL,
    first_name varchar (100) NULL,
    last_name varchar (100) NULL,
    date_of_birth Date NULL,
    class_id varchar (25) NULL,
    class_name varchar (100) NULL,
    phone varchar (100) NULL,
    email varchar (50) NULL,
    address varchar (200) NULL,
    note varchar (100) NULL,
    date_expire Date NULL
);