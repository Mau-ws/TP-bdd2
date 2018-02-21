use master
drop database comparar2
create database comparar2;
use comparar2

go
create schema com2;
go

go 
create schema com33
go

create table com2.t1
(
id int not null,
nombre varchar(20)
constraint pk_id primary key (id)
);

create table com2.t2
(
	
id int not null,
nombre2 varchar(10),
apellido varchar(20)
constraint pk_idb primary key (id)
);



create table com2.t3
(
id int not null,
nombre varchar(20),
calendario varchar(50),
constraint pk_idc primary key (id)
);





