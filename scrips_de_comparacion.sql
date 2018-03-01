use master;
go
IF EXISTS(select * from sys.databases where name='comparaciones')
DROP DATABASE comparaciones

create database comparaciones;

use comparaciones;


/*************************** Creacion de tablas para guardar la informacion de la comparacion ***********************/


create table Esquemas_bdd1
(
	esquema_que_soloEstaEnBDD1 nvarchar(max) default 'sin datos',	
);

create table Esquemas_bdd2
(
	esquema_que_soloEstaEnBDD2 nvarchar(max)default 'sin datos'
);




create table cant_tablas
(
	cant_tablas_bdd1 nvarchar(max) default 'sin datos',
	cant_tablas_bdd2 nvarchar(max) default 'sin datos'
);



create table tablas_bdd1
(
	tablas_que_soloEstaEnBDD1 nvarchar(max),
	
);

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



go
create procedure CompararBDD @bdd1 varchar(max),@bdd2 varchar(max)
as
BEGIN
set nocount on

	declare @descripcion_error varchar(max);

	begin try


		if db_id(@bdd1) is null and  db_id(@bdd2) is not null raiserror('NO SE ENCUENTRA LA BDD1',16,1)	
		if db_id(@bdd2) is null and  db_id(@bdd1) is not null raiserror('NO SE ENCUENTRA LA BDD2',16,1)
		if db_id(@bdd2) is  null and  db_id(@bdd1) is  null raiserror('NO SE ENCUENTRAN NINGUNA BDD',16,1)

			
		BEGIn
				
			
			begin	
				begin tran
				 
/************************************ NOMBRE ESQUEMAS *********************************************************/
				
				
					declare @query_esquemas nvarchar(max)
					
					declare @query_esquemas2 nvarchar(max)
				
				
					set @query_esquemas='select name 
										 from '+@bdd1+'.sys.schemas
										 where '+@bdd1+'.sys.schemas.name not in (select name
																					from '+@bdd2+'.sys.schemas)';



					set @query_esquemas2='select name 
										 from '+@bdd2+'.sys.schemas
										 where '+@bdd2+'.sys.schemas.name not in (select name
																					from '+@bdd1+'.sys.schemas)';



					
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

/**************************************************************************************************************************/

/***************************************** NOMBRE DE TABLAS DE CADA BDD****************************************************/

			
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

/**************************************************************************************************************************/

/************************************* CANTIDAD DE TABLAS DE AMBAS BDD ****************************************************/
			

			begin
				begin tran
					
					declare @cant_tablas_bdd1 nvarchar(max)
					declare @cant_tablas_bdd2 nvarchar(max)




					declare @sqlDinamico_cant_tablas nvarchar(max)
					declare @sqlDinamico_cant_tablas2 nvarchar(max)
				

					set @sqlDinamico_cant_tablas='select @aux_cant_tablas=count(object_id) 
											from '+@bdd1+'.sys.tables'

					

					
					set @sqlDinamico_cant_tablas2='select @aux_cant_tablas2=count(object_id) 
											from '+@bdd2+'.sys.tables'

					
					exec sp_executesql @sqlDinamico_cant_tablas,N'@aux_cant_tablas nvarchar(max) OUTPUT',@aux_cant_tablas=@cant_tablas_bdd1 output;
					
					exec sp_executesql @sqlDinamico_cant_tablas2,N'@aux_cant_tablas2 nvarchar(max) OUTPUT',@aux_cant_tablas2=@cant_tablas_bdd2 output;
					

					begin
					insert into cant_tablas(cant_tablas_bdd1,cant_tablas_bdd2) values
					(@cant_tablas_bdd1,@cant_tablas_bdd2);
					end


				commit tran
			end

/**************************************************************************************************************************/

/***************************************** TABLAS CON NOMBRE EN COMUN EN AMBAS BDD*****************************************/


						
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


/**************************************************************************************************************************/

/**********************************TIPOS DE DATOS EN TABLAS CON MISMO NOMBRE***********************************************/


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



/**************************************************************************************************************************/

/*************************************************** CAMPOS UNIQUE ********************************************************/

						begin
							begin tran
					
							declare @campos_unique nvarchar(max)
	
							set @campos_unique = 'SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd1+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd1+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''UNIQUE''
										  
										  UNION

										  SELECT t.CONSTRAINT_CATALOG, 
											     t.table_name,
											     c.column_name,
											     t.CONSTRAINT_TYPE 
										  FROM   '+@bdd2+'.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS C
										  inner join '+@bdd2+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										  WHERE CONSTRAINT_TYPE = ''UNIQUE'''

								begin
									insert into campos_unique(nombre_bdd,nombre_tabla,nombre_column,tipo_const)
									exec sp_executesql @campos_unique;
								end
							commit tran
							end

/**************************************************************************************************************************/

/************************************************* CANTIDAD DE COLUMNAS ***************************************************/


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

/**************************************************************************************************************************/

/******************************************************** CAMPOS DEFAULT **************************************************/

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


/**************************************************************************************************************************/

/*************************************************** CAMPOS_PK ************************************************************/
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
										  WHERE CONSTRAINT_TYPE = ''PRIMARY KEY''
										  order  by table_name'

								begin
								insert into campos_pk (nombre_bdd,nombre_tabla,nombre_column,tipo_const)
								exec sp_executesql @campos_pk;
								end
					commit tran
					end 


/**************************************************************************************************************************/

/*************************************************** CAMPOS_FK ************************************************************/
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


/**************************************************************************************************************************/

/**************************************************** CAMPOS CHECK ********************************************************/

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

/**************************************************************************************************************************/

/************************************************* CAMPOS iDENTITY ********************************************************/

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
					
		ENd
	end try


	begin catch
		IF(@@TRANCOUNT > 0)
		begin
			ROLLBACK TRAN
		end
		
		select @descripcion_error='No se pudo realizar la comparativa - '+ERROR_MESSAGE();

		insert into LOGERRORES(descripcion,mensage_de_error,line_error,error_procedimiento,fecha,id_usuario) values
		(@descripcion_error, ERROR_MESSAGE(),ERROR_LINE(), ERROR_PROCEDURE(),GETDATE(),SYSTEM_USER);


		RAISERROR (@descripcion_error,16,1);
		
	end catch
	
END



/************************* Ejecuto el procedimiento ***********************/


  exec CompararBDD 'comparar1','comparar2';



--se muestran los nombres de los esquemas de la bdd1
select  e.esquema_que_soloEstaEnBDD1 as nombre_esquemas_bdd1 from Esquemas_bdd1 e


--se muestran  los nombres de los esquemas de la bdd2
select e.esquema_que_soloEstaEnBDD2 as nombre_esquemas_bdd2 from Esquemas_bdd2 e


--se muestra  la cantidad total de tablas de ambas bdd
select t.cant_tablas_bdd1 as cantidad_total_de_tablas_bdd1,t.cant_tablas_bdd2 as cantidad_total_de_tablas_bdd2 from cant_tablas t


--se muestran las tablas con mismo nombre de ambas bdd
select nombre as tablas_con_mismo_nombre_en_ambas_bdd from mismo_nombre_Tablas

--se muestran las tablas que solo estan en la bdd1
select * from tablas_bdd1


--se muestran las tablas que solo estan en la bdd2
select * from tablas_bdd2


--se muestra la cantidad de campos que posee cada tabla con el mismo nombre en ambas bdd
select * from cant_campos 

--se muestra el tipo de dato, pocicion y si acepta null, de cada columna de las tablas con mismo nombre de ambas bdd
select * from datos_de_tablas_con_mismo_nombre



--se muestran los campos unique de cada bdd
select * from campos_unique


--se muestran los campos default de cada bdd
select * from campos_default


--se muestran los campos PK de cada bdd
select * from campos_pk


--se muestran los campos FK de cada bdd
select * from campos_fk


--se muestran los campos check de cada bdd
select *
from campos_check


--se muestran los campos identity de cada bdd
select * 
from campos_identity



--prueba logRrrores
select * 
from LOGERRORES