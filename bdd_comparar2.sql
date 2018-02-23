use master
drop database comparar2
create database comparar2;
use comparar2

go
create schema com2;
go

go 
create schema com3
go

create table com2.t1
(
id int not null,
nombre varchar(20) 
constraint pk_id primary key (id)
);

create table com3.t2
(
	
id int not null identity,
nombre2 varchar(10),
apellido varchar(20),
dni varchar (10) unique not null,
constraint pk_idb primary key (id)
);



create table com2.t3
(
id int not null ,
nombre varchar(20),
calendario varchar(50),
constraint pk_idc primary key (id)
);

create table com2.t4
(
id int not null,
nombre varchar(20),
color varchar(50) DEFAULT 'Azul',
id_t3 int,
constraint pk_id_t4 primary key (id),
constraint fk_id_t3 foreign key (id_t3) references com2.t3(id)
);


create table com2.t5
(
id_t5 int not null identity,
nombre varchar(20),
calendario varchar(50),
);



alter table com2.t1
add check (id>=1)


alter table com3.t2
add check (len(nombre2)>2)
