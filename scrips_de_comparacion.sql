use master;
go
IF EXISTS(select * from sys.databases where name='comparaciones')
DROP DATABASE comparaciones

create database comparaciones;

use comparaciones;


/**********************************************************************************/
--creacion de tablas para guardar la informacion de la comparacion


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
	nombre_bdd nvarchar(max),
	nombre_tabla nvarchar(max),
	nombre_col nvarchar(max)
)


create table cant_campos
(
	nombre_bdd nvarchar(max),
	nombre_tabla nvarchar(max),
	cant_colum nvarchar(max)
);




/*********************************************/
--ver si existen las bdd a comparar
go
create procedure ComprobarQueExistanBDD @bdd1 varchar(max),@bdd2 varchar(max)
as
BEGIN
set nocount on
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
					if (@query_esquemas is not null)
					insert into Esquemas_bdd1(esquema_que_soloEstaEnBDD1)
					execute sp_executesql @query_esquemas


					if(@query_esquemas2 is not null)
					insert into Esquemas_bdd2(esquema_que_soloEstaEnBDD2)
					execute sp_executesql @query_esquemas2
				
				
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





				if(@nombre_tablaBdd1 is not null)
				insert into tablas_bdd1(tablas_que_soloEstaEnBDD1)
				EXECUTE SP_EXECUTESQL @nombre_tablaBdd1;
				
				


				if(len(@nombre_tablaBdd2)>0)
				insert into tablas_bdd2(tablas_que_soloEstaEnBDD2)
				EXECUTE SP_EXECUTESQL @nombre_tablaBdd2;
				
				
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
					

					
					insert into cant_tablas(cant_tablas_bdd1,cant_tablas_bdd2) values
					(@cant_tablas_bdd1,@cant_tablas_bdd2);



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


							insert into mismo_nombre_Tablas(nombre)
							exec sp_executesql @queryDinamico;

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


							insert into datos_de_tablas_con_mismo_nombre(nombre_bdd,nombre_tablas_en_comun,nombre_columnas,tipo_dato,permite_null,posicion_en_tabla)
							exec sp_executesql @datos_tablas_mismo_nombre;

						commit tran
					end


	/************************************************************************************************/


	/*CAMPOS_UNIQUE*/

	begin
		begin tran
					
		declare @campos_unique nvarchar(max)
	
		set @campos_unique ='select u.TABLE_CATALOG,
									u.CONSTRAINT_SCHEMA,
									u.CONSTRAINT_TYPE
							 from '+@bdd1+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS U
							 where U.CONSTRAINT_TYPE = ''UNIQUE''
							 
							 UNION
							 select u.TABLE_CATALOG,
									u.CONSTRAINT_SCHEMA,
									u.CONSTRAINT_TYPE
							from '+@bdd2+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS U
							where U.CONSTRAINT_TYPE = ''UNIQUE'''

		insert into campos_unique(nombre_bdd,nombre_tabla,nombre_col)
		exec sp_executesql @campos_unique;
		commit tran
		end

	/************************************************************************************************/

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




											insert into cant_campos(nombre_bdd,nombre_tabla,cant_colum)
											execute sp_executesql @cant_campos


						commit tran
					end









		/************************************************************************************************/
		ENd
	end try


	begin catch
		IF(@@TRANCOUNT > 0)
		BEGIN
			-- HACEMOS ROLLBACK DE TODAS LAS TRANSACCIONES ANTERIORES
			ROLLBACK TRAN

			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;

			
		SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();

			RAISERROR (@ErrorMessage, -- Message text.
				   16, -- Severity.
				   @ErrorState -- State.
				  );
		ENd	
	end catch
	
END



--ejecuto el procedimiento


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






select t.TABLE_CATALOG as BDD,t.TABLE_NAME as nombre_tabla, count(t.COLUMN_NAME) as cantidad_de_columnas
from comparar1.INFORMATION_SCHEMA.COLUMNS t
where t.TABLE_NAME in(select TABLE_NAME from comparar2.INFORMATION_SCHEMA.COLUMNS)
group by t.TABLE_NAME,t.TABLE_CATALOG

union all

select t.TABLE_CATALOG,t.TABLE_NAME, count(t.COLUMN_NAME)
from comparar2.INFORMATION_SCHEMA.COLUMNS t
where t.TABLE_NAME in(select TABLE_NAME from comparar1.INFORMATION_SCHEMA.COLUMNS)
group by t.TABLE_NAME,t.TABLE_CATALOG

order by TABLE_NAME









/*
select a.table_catalog as nombre_de_bdd,
		a.TABLE_NAME as nombre_tablas_en_comun,
		a.COLUMN_NAME as nombre_columna,
		a.DATA_TYPE as tipo_dato,
		a.IS_NULLABLE as permite_null,
		a.ORDINAL_POSITION as posicion_en_tabla
from comparar1.INFORMATION_SCHEMA.COLUMNS a
where a.TABLE_NAME in (select TABLE_NAME
						from comparar2.INFORMATION_SCHEMA.COLUMNS)
						
union all
select a.table_catalog,
		a.TABLE_NAME ,
		a.COLUMN_NAME, 
		a.DATA_TYPE,
		a.IS_NULLABLE, 
		a.ORDINAL_POSITION 
from comparar2.INFORMATION_SCHEMA.COLUMNS a
where a.TABLE_NAME in (select TABLE_NAME
						from comparar1.INFORMATION_SCHEMA.COLUMNS)
						order by TABLE_NAME




*/



/*

--tipo de datos,de las columnas en tablas de mismo nombre
select a.table_catalog as nombre_de_bdd,a.TABLE_NAME,a.COLUMN_NAME,a.DATA_TYPE
from comparar1.INFORMATION_SCHEMA.COLUMNS a


select *
from comparar1.INFORMATION_SCHEMA.KEY_COLUMN_USAGE


*/





/*

/**********************************/
--lo siguiente son codigos de pruebas para tenerlos de referencia, hay que sacarlos luego

select DB_ID('comparar2');


select DB_ID('comparar1');




 */
 --devuelve el nombre de la columna de una tabla
 /*
select COL_NAME(object_id('cant_tablas'),1)


*/
/*

SELECT count(*) as cantidad_de_columnas
FROM information_schema.columns
WHERE table_name = 'cant_tablas'
*/






/*************************************************/

--info bdd1
select  t.table_catalog as nombre_de_bdd,
		t.TABLE_NAME as nombre_tabla,
		t.COLUMN_NAME as nombre_columna,
		t.DATA_TYPE as tipo_dato,
		t.IS_NULLABLE as es_null,
		t.ORDINAL_POSITION as posicion_en_tabla
from comparar1.INFORMATION_SCHEMA.columns t 
where t.TABLE_NAME in (select i.TABLE_NAME
						from comparar2.INFORMATION_SCHEMA.COLUMNS i)
			order by TABLE_NAME			
		
					







select a.TABLE_CATALOG as bdd1, count(a.TABLE_NAME)
from comparar1.INFORMATION_SCHEMA.COLUMNS a
group by TABLE_CATALOG





select *
from comparar1.INFORMATION_SCHEMA.columns


select *
from comparar1.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
union
select *
from comparar2.INFORMATION_SCHEMA.TABLE_CONSTRAINTS








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