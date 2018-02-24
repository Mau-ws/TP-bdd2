use master;
go
IF EXISTS(select * from sys.databases where name='comparaciones')
DROP DATABASE comparaciones

create database comparaciones;

use comparaciones;


/*************************** Creacion de tablas para guardar la informacion de la comparacion ***********************/



--nombre de esquemas de la bdd1
create table Esquemas_bdd1
(
	esquema_que_soloEstaEnBDD1 nvarchar(max) default 'sin datos',	
);

--nombre de esquemas de la bdd2
create table Esquemas_bdd2
(
	esquema_que_soloEstaEnBDD2 nvarchar(max)default 'sin datos'
);




--cantidad de tablas de ambas bdd, sin contar  las del sistema
create table cant_tablas
(
	cant_tablas_bdd1 nvarchar(max) default 'sin datos',
	cant_tablas_bdd2 nvarchar(max) default 'sin datos'
);



--nombre de tablas que solo estan en la bdd 1
create table tablas_bdd1
(
	tablas_que_soloEstaEnBDD1 nvarchar(max),
	
);

--nombre de tablas que solo estan en la bdd 2
create table tablas_bdd2
(
	tablas_que_soloEstaEnBDD2 nvarchar(max) ,
);

create table mismo_nombre_Tablas
(
	nombre nvarchar(max) ,

)

create table datos_de_tablas_con_mismo_nombre
(
	nombre_bdd nvarchar(max),
	nombre_tablas_en_comun nvarchar(max),
	nombre_columnas nvarchar(max),
	tipo_dato nvarchar(max),
	permite_null nvarchar(max),
	posicion_en_tabla int
)

create table campos_unique
(
	nombre_bdd nvarchar (max),
	nombre_tabla nvarchar (max),
	nombre_column nvarchar (max),
	tipo_const nvarchar (max)
)


create table cant_campos
(
	nombre_bdd nvarchar(max),
	nombre_tabla nvarchar(max),
	cant_colum nvarchar(max)
);


create table campos_default
(
	nombre_bdd nvarchar (max),
	nombre_tabla nvarchar (max),
	nombre_column nvarchar (max),
	dato_default nvarchar (max)
);

create table campos_pk
(
	nombre_bdd nvarchar (max),
	nombre_tabla nvarchar (max),
	nombre_column nvarchar (max),
	tipo_const nvarchar (max)
)

create table campos_fk
(
	nombre_bdd nvarchar (max),
	nombre_tabla nvarchar (max),
	nombre_column nvarchar (max),
	tipo_const nvarchar (max)
)


create table campos_check
(
	BDD  nvarchar(max),
	esquema nvarchar(max),
	tabla nvarchar(max),
	tipo nvarchar(max),
	columna nvarchar(max)
)

create table campos_identity
(
	bdd_name nvarchar(max),
	esquema nvarchar (max),
	tabla nvarchar(max),
	columna nvarchar(max),
	es_identity nvarchar(max)
);



create table LOGERRORES
(
	id int identity,
	descripcion varchar(max),
	mensage_de_error nvarchar(max),
	error_procedimiento nvarchar(max),
	line_error int,
	id_usuario varchar(max),
	fecha datetime
)


/************************* Fin creacion de tablas para guardar la informacion de la comparacion *******************/

/************************* Ver si existen las bdd a comparar *******************/


go
create procedure ComprobarQueExistanBDD @bdd1 varchar(max),@bdd2 varchar(max)
as
BEGIN
set nocount on
	declare @descripcion_error varchar(max);
	begin try

		--veo si existen las bdd, y manda un mensaje dependiendo cual no esta
		if db_id(@bdd1) is null and  db_id(@bdd2) is not null raiserror('NO SE ENCUENTRA LA BDD1',16,1)	
		if db_id(@bdd2) is null and  db_id(@bdd1) is not null raiserror('NO SE ENCUENTRA LA BDD2',16,1)
		if db_id(@bdd2) is  null and  db_id(@bdd1) is  null raiserror('NO SE ENCUENTRAN NINGUNA BDD',16,1)

			
		BEGIn
				/************************************************************************************************/
			--bloque que compara el nombre de los esquemas
			begin	
				begin tran

				
					--creo una variable para guardar la consulta como un texto
					declare @query_esquemas nvarchar(max)
					
					declare @query_esquemas2 nvarchar(max)
				
					
					/*cuando se ejecuta con el sp_executesql, el auxiliar pasa a tener el dato de la consulta
					aca ocurre la magia negra*/
					set @query_esquemas='select name 
										 from '+@bdd1+'.sys.schemas
										 where '+@bdd1+'.sys.schemas.name not in (select name
																					from '+@bdd2+'.sys.schemas)';



					set @query_esquemas2='select name 
										 from '+@bdd2+'.sys.schemas
										 where '+@bdd2+'.sys.schemas.name not in (select name
																					from '+@bdd1+'.sys.schemas)';



				/*valido que los que traiga la consulta no se a null, solo asi se guarda en la tabla
				*/
					
					begin
						insert into Esquemas_bdd1(esquema_que_soloEstaEnBDD1)
						execute sp_executesql @query_esquemas
					end

					
					begin
						insert into Esquemas_bdd2(esquema_que_soloEstaEnBDD2)
						execute sp_executesql @query_esquemas2
					end			
				
				commit tran
			end 
				/***********************************************************************************************************/

			--transaccion para comparar tablas por nombre
			begin 
				begin tran

				declare @nombre_tablaBdd1 nvarchar(max)
				declare @nombre_tablaBdd2 nvarchar(max)

				set @nombre_tablaBdd1='select name 
								   from '+@bdd1+'.sys.tables
								   where '+@bdd1+'.sys.tables.name not in (select name
																			from '+@bdd2+'.sys.tables);';


				
				set @nombre_tablaBdd2='select name 
								   from '+@bdd2+'.sys.tables
								   where '+@bdd2+'.sys.tables.name not in (select name
																			from '+@bdd1+'.sys.tables);';





				begin
					insert into tablas_bdd1(tablas_que_soloEstaEnBDD1)
					EXECUTE SP_EXECUTESQL @nombre_tablaBdd1;
				end
				

				begin
					insert into tablas_bdd2(tablas_que_soloEstaEnBDD2)
					EXECUTE SP_EXECUTESQL @nombre_tablaBdd2;
				end				
				commit tran
				
			end
				/************************************************************************************************/
			

			--transaccion para comparar cantidad de tablas
			begin
				begin tran
					
					declare @cant_tablas_bdd1 nvarchar(max)
					declare @cant_tablas_bdd2 nvarchar(max)




					declare @sqlDinamico_cant_tablas nvarchar(max)
					declare @sqlDinamico_cant_tablas2 nvarchar(max)
				

					set @cant_tablas_bdd1='select @aux_cant_tablas=count(object_id) 
											from '+@bdd1+'.sys.tables'

					

					
					set @cant_tablas_bdd2='select @aux_cant_tablas2=count(object_id) 
											from '+@bdd2+'.sys.tables'


					
					/* forma de meter varias columnas en una tabla, el problema es que no soporta varias finasl, 
					por eso no inclui éste método en las consultas de arriba, ya que las demas traian mas de una fila */
					
					exec sp_executesql @cant_tablas_bdd1,N'@aux_cant_tablas nvarchar(max) OUTPUT',@aux_cant_tablas=@cant_tablas_bdd1 output;
					
					exec sp_executesql @cant_tablas_bdd2,N'@aux_cant_tablas2 nvarchar(max) OUTPUT',@aux_cant_tablas2=@cant_tablas_bdd2 output;
					

					begin
					insert into cant_tablas(cant_tablas_bdd1,cant_tablas_bdd2) values
					(@cant_tablas_bdd1,@cant_tablas_bdd2);
					end


				commit tran
			end
				/************************************************************************************************/

						--llenar mismo_nombre_Tablas
					begin
						begin tran
					
							declare @queryDinamico nvarchar(max)
	
							set @queryDinamico ='select t.TABLE_NAME 
												from '+@bdd1+'.INFORMATION_SCHEMA.columns t
												where t.TABLE_NAME in (select i.TABLE_NAME
																		from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS i)
																	group by TABLE_NAME'

						begin
							insert into mismo_nombre_Tablas(nombre)
							exec sp_executesql @queryDinamico;
						end

						commit tran
					end


	/************************************************************************************************/


				--rellenar datos_de_tablas_con_mismo_nombre
				begin
						begin tran
					
							declare @datos_tablas_mismo_nombre nvarchar(max)
	
							set @datos_tablas_mismo_nombre ='select a.table_catalog ,
																		a.TABLE_NAME ,
																		a.COLUMN_NAME ,
																		a.DATA_TYPE ,
																		a.IS_NULLABLE ,
																		a.ORDINAL_POSITION 
																from '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS a
																where a.TABLE_NAME in (select TABLE_NAME
																						from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS)
						
																union all
																select a.table_catalog,
																		a.TABLE_NAME ,
																		a.COLUMN_NAME, 
																		a.DATA_TYPE,
																		a.IS_NULLABLE, 
																		a.ORDINAL_POSITION 
																from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS a
																where a.TABLE_NAME in (select TABLE_NAME
																						from '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS)
																						order by TABLE_NAME
'

							begin
								insert into datos_de_tablas_con_mismo_nombre(nombre_bdd,nombre_tablas_en_comun,nombre_columnas,tipo_dato,permite_null,posicion_en_tabla)
								exec sp_executesql @datos_tablas_mismo_nombre;
							end
						commit tran
					end


	/************************************************************************************************/


/************************************ campos_unique *****************************************/

						begin
							begin tran
					
							declare @campos_unique nvarchar(max)
	
							set @campos_unique = 'SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd1+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join comparar1.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''UNIQUE''
										  
										  UNION

										  SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd2+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''UNIQUE'''

								begin
									insert into campos_unique(nombre_bdd,nombre_tabla,nombre_column,tipo_const)
									exec sp_executesql @campos_unique;
								end
							commit tran
							end

/************************************ campos_unique *****************************************/


						--transaccion 
						begin
							begin tran

						declare @cant_campos nvarchar(max);

						set @cant_campos='select t.TABLE_CATALOG ,t.TABLE_NAME, count(t.COLUMN_NAME)
											from '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS t
											where t.TABLE_NAME in(select TABLE_NAME from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS)
											group by t.TABLE_NAME,t.TABLE_CATALOG

											union all

											select t.TABLE_CATALOG,t.TABLE_NAME, count(t.COLUMN_NAME)
											from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS t
											where t.TABLE_NAME in(select TABLE_NAME from '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS)
											group by t.TABLE_NAME,t.TABLE_CATALOG

											order by TABLE_NAME'



										begin
											insert into cant_campos(nombre_bdd,nombre_tabla,cant_colum)
											execute sp_executesql @cant_campos

										end
						commit tran
					end


/************************************ campos_default ************************************/

					begin
						begin tran
						declare @campos_default nvarchar (max);

						set @campos_default = 'SELECT D.TABLE_CATALOG,
													  D.TABLE_NAME,
													  D.COLUMN_NAME,
													  D.COLUMN_DEFAULT
											   FROM '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS D
											   WHERE D.COLUMN_DEFAULT IS NOT NULL
						   
											   UNION
						   
											   SELECT D.TABLE_CATALOG,
													  D.TABLE_NAME,
													  D.COLUMN_NAME,
													  D.COLUMN_DEFAULT
											   FROM '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS D
											   WHERE D.COLUMN_DEFAULT IS NOT NULL' 

							begin
								insert into campos_default (nombre_bdd, nombre_tabla, nombre_column,dato_default)
								exec sp_executesql @campos_default;
							end
					commit tran
					end

/************************************ campos_default ************************************/

/************************************ campos_pk *****************************************/
					begin
						begin tran
						declare @campos_pk nvarchar (max);

						set @campos_pk = 'SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd1+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd1+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''PRIMARY KEY''
										  
										  UNION

										  SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd2+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd2+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''PRIMARY KEY'''


								begin
								insert into campos_pk (nombre_bdd,nombre_tabla,nombre_column,tipo_const)
								exec sp_executesql @campos_pk;
								end
					commit tran
					end 

/************************************ campos_pk *****************************************/

/************************************ campos_fk *****************************************/
					begin
						begin tran
						declare @campos_fk nvarchar (max);

						set @campos_fk = 'SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd1+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd1+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''FOREIGN KEY''
										  
										  UNION

										  SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd2+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd2+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''FOREIGN KEY'''
					
					begin
						insert into campos_fk (nombre_bdd,nombre_tabla,nombre_column,tipo_const)
						exec sp_executesql @campos_fk;
					end
					commit tran
					end 

/************************************ campos_fk *****************************************/



/**************************************campos check********************************************/

						begin
							begin tran
							declare @campo_check nvarchar(max)

							set @campo_check='select t.CONSTRAINT_CATALOG,t.CONSTRAINT_SCHEMA,t.TABLE_NAME,t.CONSTRAINT_TYPE,c.COLUMN_NAME
											from '+@bdd1+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t inner join
												 '+@bdd1+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c on t.CONSTRAINT_NAME=c.CONSTRAINT_NAME
												 where CONSTRAINT_TYPE =''CHECK''
												 and t.TABLE_NAME in(select TABLE_NAME
																		from '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS)

												 union

											select t.CONSTRAINT_CATALOG,t.CONSTRAINT_SCHEMA,t.TABLE_NAME,t.CONSTRAINT_TYPE,c.COLUMN_NAME
											from '+@bdd2+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t inner join
												 '+@bdd2+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c on t.CONSTRAINT_NAME=c.CONSTRAINT_NAME
												 where CONSTRAINT_TYPE =''CHECK''
												 and t.TABLE_NAME in(select TABLE_NAME
																		from '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS)
												 order by TABLE_NAME'

							begin
									insert into campos_check(BDD,esquema,tabla,tipo,columna)
									exec sp_executesql @campo_check
							end
							commit
						end
/**********************************************************************************************/

						begin
							begin tran

							declare @campos_identity nvarchar(max);



							set @campos_identity='select  distinct c.TABLE_CATALOG, s.name ,t.name  ,i.name , case when i.is_identity=1 then ''Identity'' end as Es_IDENTITY
												from '+@bdd1+'.sys.tables t inner join '+@bdd1+'.sys.identity_columns i on t.object_id=i.object_id
												inner join '+@bdd1+'.sys.schemas s on s.schema_id in (t.schema_id)	
												inner join '+@bdd1+'.INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA=s.name			
							
												where i.is_identity=1


												union
												select  distinct c.TABLE_CATALOG, s.name as esquema,t.name as tabla ,i.name as columna, case when i.is_identity=1 then ''Identity'' end as Es_IDENTITY
												from '+@bdd2+'.sys.tables t inner join '+@bdd2+'.sys.identity_columns i on t.object_id=i.object_id
												inner join '+@bdd2+'.sys.schemas s on s.schema_id in (t.schema_id)	
												inner join '+@bdd2+'.INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA=s.name			
							
												where i.is_identity=1
'
							
							begin
								insert into campos_identity(bdd_name,esquema,tabla,columna,es_identity)
								exec sp_executesql @campos_identity
							end
							
						commit tran
					end

/**********************************************************************************************/
		ENd
	end try


	begin catch
		IF(@@TRANCOUNT > 0)
		begin
			ROLLBACK TRAN
		end
		
		select @descripcion_error='no se pudo insertar los datos - '+ERROR_MESSAGE();

		insert into LOGERRORES(descripcion,mensage_de_error,line_error,error_procedimiento,fecha,id_usuario) values
		(@descripcion_error, ERROR_MESSAGE(),ERROR_LINE(), ERROR_PROCEDURE(),GETDATE(),SYSTEM_USER);


		RAISERROR (@descripcion_error,16,1);
		
	end catch
	
END


/************************* Ejecuto el procedimiento *******************/



exec ComprobarQueExistanBDD 'comparar1','comparar2';



--traigo la informaciona comparar
select * from Esquemas_bdd1

select * from Esquemas_bdd2

select t.cant_tablas_bdd1 as cantidad_total_de_tablas_bdd1, t.cant_tablas_bdd2 as cantidad_total_de_tablas_bdd2 from cant_tablas t

select * from tablas_bdd1

select * from tablas_bdd2

select nombre as tablas_con_mismo_nombre_en_ambas_bdd from mismo_nombre_Tablas


select * 
from datos_de_tablas_con_mismo_nombre

select * from campos_unique

select * from cant_campos

select * from campos_default

select * from campos_pk

select * from campos_fk

select *
from campos_check


select * 
from campos_identity












SELECT * FROM comparar2.sys.syscolumns WHERE colstat = 1

select *
from comparar1.sys.identity_columns


 

select *
from comparar1.sys.identity_columns
where object_id not in(1993058136,2025058250,2057058364)

select *
from comparar1.sys.identity_columns
where object_id not in(1993058136,2025058250,2057058364)







select  distinct c.TABLE_CATALOG, s.name as esquema,t.name as tabla ,i.name as columna, case when i.is_identity=1 then 'Identity' end as Es_IDENTITY
from comparar2.sys.tables t inner join comparar2.sys.identity_columns i on t.object_id=i.object_id
							inner join comparar2.sys.schemas s on s.schema_id in (t.schema_id)	
							inner join comparar2.INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA=s.name			
							
where i.is_identity=1


union
select  distinct c.TABLE_CATALOG, s.name as esquema,t.name as tabla ,i.name as columna, case when i.is_identity=1 then 'Identity' end as Es_IDENTITY
from comparar1.sys.tables t inner join comparar1.sys.identity_columns i on t.object_id=i.object_id
							inner join comparar1.sys.schemas s on s.schema_id in (t.schema_id)	
							inner join comparar1.INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA=s.name			
							
where i.is_identity=1










select*
from comparar1.INFORMATION_SCHEMA.TABLE_CONSTRAINTS

select *
from comparar1.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE









--check

/*
select t.CONSTRAINT_CATALOG,t.CONSTRAINT_SCHEMA,t.TABLE_NAME,t.CONSTRAINT_TYPE,c.COLUMN_NAME
from comparar1.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t inner join
	 comparar1.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c on t.CONSTRAINT_NAME=c.CONSTRAINT_NAME
	 where CONSTRAINT_TYPE ='CHECK'
	 and t.TABLE_NAME in(select TABLE_NAME
							from comparar2.INFORMATION_SCHEMA.COLUMNS)

	 union

select t.CONSTRAINT_CATALOG,t.CONSTRAINT_SCHEMA,t.TABLE_NAME,t.CONSTRAINT_TYPE,c.COLUMN_NAME
from comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t inner join
	 comparar2.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c on t.CONSTRAINT_NAME=c.CONSTRAINT_NAME
	 where CONSTRAINT_TYPE ='CHECK'
	 and t.TABLE_NAME in(select TABLE_NAME
							from comparar1.INFORMATION_SCHEMA.COLUMNS)
	 order by TABLE_NAME


	 */





--traer indices
select * from comparar1.sys.indexes

										 

/*************************************************/



















/*
--traigo los esquemas de una bdd que no se encuentra en la otra		

select name 
from comparar2.sys.schemas
where comparar2.sys.schemas.name not in (select name
									from comparar1.sys.schemas);





					--traigo la cantidad de tablas de una bdd
					 select count(object_id) as cant_tablas
					from comparar1.sys.tables



					--traigo los nombre de las tablas

					select name
					from comparar1.sys.tables
				



					--traigo esquemas
					select * from sys.schemas;

					--selecciono el nombre del esquema
					select name from comparar1.sys.schemas
					where comparar1.sys.schemas.name='com1';



---comparar campos:



select name as nombre_tablas_queNoEstanEnLaBdd2
								   from comparar1.sys.tables
								   where comparar1.sys.tables.name not in (select name
																			from comparar2.sys.tables);



--busco el nombre de una tabla 

select *
from comparar1.sys.tables as t 
where t.name in('t2','t4');


--busco el nombre de las tablas que pertenecen al mismo esquema
select t.name
from comparar1.sys.tables as t inner join comparar1.sys.schemas as s on t.schema_id=s.schema_id
where  s.schema_id=5;





	
--------------------------------
/
DECLARE @variable TABLE 
(esquema_que_soloEstaEnBDD1 nvarchar(max))


insert into @variable 
select name 
	from comparar2.sys.schemas
	where comparar2.sys.schemas.name not in (select name
									from comparar1.sys.schemas)




insert into Esquemas_bdd2(esquema_que_soloEstaEnBDD2)
select * from @variable
*/
-------------------------------------------




/************************************************************************/
/*
DECLARE @Catalogos TABLE
(
	Id INT,
	Descripcion NVARCHAR(100), 
	Abreviatura NVARCHAR(20), 
	Comentarios NVARCHAR(200)
)

SET @Query = ' SELECT ' + @Campo + ', Descripcion, Abreviatura, Comentarios '
SET @Query = @Query + ' FROM ' + @Tabla + ' WHERE Descripcion LIKE ''%' + @Filtro + '%''' 
INSERT INTO @Catalogos
EXECUTE SP_EXECUTESQL @Query

SELECT Id, Descripcion, Abreviatura, Comentarios FROM @Catalogos

-------------------------------------------------------------------------
			

					declare @bdd5 varchar(max)='comparar1'
					declare @bdd6 varchar(max)='comparar2'


					--creo una variable para guardar la consulta como un texto
					declare @query nvarchar(max)
				
					
					/*cuando se ejecuta con el sp_executesql, el auxiliar pasa a tener el dato de la consulta
					aca ocurre la magia negra*/
					set @query='select  name 
										 from '+@bdd6+'.sys.schemas
										 where '+@bdd6+'.sys.schemas.name not in (select name
																					from '+@bdd5+'.sys.schemas);'



					if(@query is not null)					
					insert into Esquemas(esquema_que_soloEstaEnBDD2)
					exec sp_executesql @query




					select * from Esquemas*/




		/****************************************************************************************/

			--transaccion para traer las tablas que estan en ambas bdd
		/*	begin
				begin tran
					
					declare @tablas_mismo_nombre nvarchar(max)
	
					set @tablas_mismo_nombre ='select name
												from '+@bdd1+'.sys.tables
												where '+@bdd1+'.sys.tables.name in(select name
																					from '+@bdd2+'.sys.tables)'


					insert into tablas_en_comun(nombre_tablas)
					exec sp_executesql @tablas_mismo_nombre;

				commit tran
			end*/
			/************************************************************************************************/


			
			--transaccion para trar la cantidad de columnas en comun para tablas con mismo nombre
		/*	begin
				begin tran
					
					declare @cant_columnas nvarchar(max)
	
					set @tablas_mismo_nombre ='select name
												from '+@bdd1+'.sys.tables
												where '+@bdd1+'.sys.tables.name in(select name
																					from '+@bdd2+'.sys.tables)'


					insert into tablas_en_comun(nombre_tablas)
					exec sp_executesql @tablas_mismo_nombre;

				commit tran
			end*/
				/************************************************************************************************/


/*

select *
from comparar1.INFORMATION_SCHEMA.COLUMNS t
where t.TABLE_SCHEMA not in(select TABLE_SCHEMA
								from comparar2.INFORMATION_SCHEMA.columns)

	
select * from comparar1.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
select * from comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS where CONSTRAINT_TYPE = 'UNIQUE'
*/
go
SELECT t.CONSTRAINT_CATALOG, 
	   t.table_name,
	   c.column_name,
	   t.CONSTRAINT_TYPE 
FROM   comparar2.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
inner join comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME

SELECT * FROM comparar1.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
select * from comparar1.INFORMATION_SCHEMA.TABLE_CONSTRAINTS

SELECT t.CONSTRAINT_CATALOG, 
	   t.table_name,
	   c.column_name,
	   t.CONSTRAINT_TYPE 
FROM   comparar2.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
inner join comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'