drop database comparar1
create database comparar1;
use comparar1;

go
create schema com1;
go

create table com1.t1
(
id int not null,
nombre varchar(20)
constraint pk_id primary key (id)
);

create table com1.t2
(
id int not null,
nombre2 varchar(10)
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







