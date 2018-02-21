use master
drop database comparar1
create database comparar1;
use comparar1;

go
create schema com1;
go

create table com1.t1
(
ids int primary key,
nombres varchar(20)
);

create table com1.t2
(

nombre2 varchar(10) not null,
id int not null,
constraint pk_idb primary key (id)
);



create table com1.t3
(
id int not null,
nombre varchar(20)
constraint pk_idc primary key (id)
);

--agrego una tabla de mas para comprobar validacion por nombre de tabla
create table com1.t4
(
id int not null,
nombre varchar(20)
constraint pk_idd primary key (id)
);



create table com1.t5
(
id int not null,
nombre varchar(20)
constraint pk_ide primary key (id)
);









