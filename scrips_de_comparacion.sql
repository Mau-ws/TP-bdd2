use master;
go
IF EXISTS(select * from sys.databases where name='comparaciones')
DROP DATABASE comparaciones

create database comparaciones;

use comparaciones;


/**********************************************************************************/
--creacion de tablas para guardar la informacion de la comparacion

create table cant_tablas
(
	cant_tablas_bdd1 nvarchar(max),
	cant_tablas_bdd2 nvarchar(max)
);

create table tablas_bdd1
(
	tablas_que_soloEstaEnBDD1 nvarchar(max),
	
);

create table tablas_bdd2
(
	tablas_que_soloEstaEnBDD2 nvarchar(max),
);



create table Esquemas_bdd1
(
	esquema_que_soloEstaEnBDD1 nvarchar(max),	
);

create table Esquemas_bdd2
(
	esquema_que_soloEstaEnBDD2 nvarchar(max)
);


create table tablas_en_comun
(
	nombre_tablas nvarchar(max)
);


create table cant_columnas
(
	columnas_iguales nvarchar(max),
)



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
			---------------------------------------------------------------
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
			------------------------------------------------------------------



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
				
				


				if(@nombre_tablaBdd2 is not null)
				insert into tablas_bdd2(tablas_que_soloEstaEnBDD2)
				EXECUTE SP_EXECUTESQL @nombre_tablaBdd2;
				
				
				commit tran
				
			end
			------------------------------------------------------------------------
			

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
			------------------------------------------------------------------------------------

			--transaccion para traer las tablas que estan en ambas bdd
			begin
				begin tran
					
					declare @tablas_mismo_nombre nvarchar(max)
	
					set @tablas_mismo_nombre ='select name
												from '+@bdd1+'.sys.tables
												where '+@bdd1+'.sys.tables.name in(select name
																					from '+@bdd2+'.sys.tables)'


					insert into tablas_en_comun(nombre_tablas)
					exec sp_executesql @tablas_mismo_nombre;

				commit tran
			end
			------------------------------------------------------------------------------------


			
			--transaccion para traer las tablas que estan en ambas bdd
			begin
				begin tran
					
					declare @cant_columnas nvarchar(max)
	
					set @tablas_mismo_nombre ='select name
												from '+@bdd1+'.sys.tables
												where '+@bdd1+'.sys.tables.name in(select name
																					from '+@bdd2+'.sys.tables)'


					insert into tablas_en_comun(nombre_tablas)
					exec sp_executesql @tablas_mismo_nombre;

				commit tran
			end
			------------------------------------------------------------------------------------
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




select * from Esquemas_bdd1

select * from Esquemas_bdd2

select * from cant_tablas

select * from tablas_bdd1

select * from tablas_bdd2

select t.nombre_tablas as Tablas_en_comun_entre_las_bases_de_datos
from tablas_en_comun t group by nombre_tablas;

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





--mostrar nombre de tablas que estan en ambas bdd
select t.TABLE_NAME as tablas_en_ambas_bdd,COLUMN_NAME as columnas_iguales
from comparar1.INFORMATION_SCHEMA.columns t
where t.TABLE_NAME in (select i.TABLE_NAME
						from comparar2.INFORMATION_SCHEMA.COLUMNS i)and t.COLUMN_NAME in(select c.COLUMN_NAME
																						from comparar2.INFORMATION_SCHEMA.COLUMNS c)
order by t.TABLE_NAME






--mostrar cantidad de tablas

select t.TABLE_NAME as tablas_mismo_nombre,count(COLUMN_NAME) as cant_columnas_iguales
from comparar1.INFORMATION_SCHEMA.columns t
where t.TABLE_NAME in (select i.TABLE_NAME
						from comparar2.INFORMATION_SCHEMA.COLUMNS i)and t.COLUMN_NAME in(select c.COLUMN_NAME
																						from comparar2.INFORMATION_SCHEMA.COLUMNS c)
group by TABLE_NAME






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







