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

create table Esquemas
(
	esquema_que_soloEstaEnBDD1 nvarchar(max),
	
	esquema_que_soloEstaEnBDD2 nvarchar(max)
	
);





/*********************************************/
--ver si existen las bdd a comparar
go
create procedure ComprobarQueExistanBDD @bdd1 varchar(max),@bdd2 varchar(max)
as
BEGIN
set nocount on
	begin try
		--veo si existen las bdd, y mansa un mensaje dependiendo cual no esta
		if db_id(@bdd1) is null and  db_id(@bdd2) is not null raiserror('NO SE ENCUENTRA LA BDD1',16,1)	
		if db_id(@bdd2) is null and  db_id(@bdd1) is not null raiserror('NO SE ENCUENTRA LA BDD2',16,1)
		if db_id(@bdd2) is  null and  db_id(@bdd1) is  null raiserror('NO SE ENCUENTRAN NINGUNA BDD',16,1)

			
			BEGIN
			---------------------------------------------------------------
			--bloque que compara el nombre de los esquemas
			begin	
				begin tran

					--variables que guardan el resultado de lo que trae el @sqlDinamico
					declare @esquema_bdd1 nvarchar(max)
					
					declare @esquema_bdd2 nvarchar(max)
					


					--creo una variable para guardar la consulta como un texto
					declare @sqlDinamico nvarchar(max)
					
					declare @sqlDinamico2 nvarchar(max)
				
					
					/*cuando se ejecuta con el sp_executesql, el auxiliar pasa a tener el dato de la consulta
					aca ocurre la magia negra*/
					set @sqlDinamico='select @auxiliar1= name 
										 from '+@bdd1+'.sys.schemas
										 where '+@bdd1+'.sys.schemas.name not in (select name
																					from '+@bdd2+'.sys.schemas);'



					set @sqlDinamico2='select @auxiliar2= name 
										 from '+@bdd2+'.sys.schemas
										 where '+@bdd2+'.sys.schemas.name not in (select name
																					from '+@bdd1+'.sys.schemas);'

				


					

				--paso a aclarar:
				/* al ejecutar el @sqlDinamico con el 'sp_executesql', lo que hace es transformar lo que está en string
				enm formato codigo, quedaria como una cosulta comun, el @auxiliar, es una variable la cual mantiene 
				el resultado de la consulta, con el output, mantengo el resultado, y lo puedo igualar a otra 
				variable para poder setearlo luego
				*/

					execute sp_executesql @sqlDinamico ,N'@auxiliar1 nvarchar(max) OUTPUT',@auxiliar1=@esquema_bdd1 output


				    execute sp_executesql @sqlDinamico2 ,N'@auxiliar2 nvarchar(max) OUTPUT',@auxiliar2=@esquema_bdd2 output;
				
				

				--inserto los datos.
				--el problemas es que solo trae un registro
				--en comparar2, hay 2 esquemas, pero solo trae uno 
				insert into Esquemas(esquema_que_soloEstaEnBDD1,esquema_que_soloEstaEnBDD2)
				select @esquema_bdd1,@esquema_bdd2
				
				commit tran
			end 
			------------------------------------------------------------------


			--transaccion para comparar tablas por nombre
			begin 
				begin tran

				declare @nombre_tablaBdd1 nvarchar(max)
				declare @nombre_tablaBdd2 nvarchar(max)

				set @nombre_tablaBdd1='select name as tablas_bdd1_queNoEstanEnLa2
								   from '+@bdd1+'.sys.tables
								   where '+@bdd1+'.sys.tables.name not in (select name
																			from '+@bdd2+'.sys.tables);';


				
				set @nombre_tablaBdd2='select name as tablas_bdd2_queNoEstanEnLa1
								   from '+@bdd2+'.sys.tables
								   where '+@bdd2+'.sys.tables.name not in (select name
																			from '+@bdd1+'.sys.tables);';





				if(@nombre_tablaBdd1 is not null)
				begin
				EXECUTE SP_EXECUTESQL @nombre_tablaBdd1;
				end

				if(len(@nombre_tablaBdd2)>0)
				EXECUTE SP_EXECUTESQL @nombre_tablaBdd2;
			
				commit tran
			end
			------------------------------------------------------------------------
			
			--transaccion para comparar cantidad de tablas
			begin
				begin tran
					
					declare @cant_tablas_bdd1 nvarchar(max)
					declare @cant_tablas_bdd2 nvarchar(max)

					set @cant_tablas_bdd1='select count(object_id) as cant_tablas
											from '+@bdd1+'.sys.tables'

					


					set @cant_tablas_bdd2='select count(object_id) as cant_tablas
											from '+@bdd2+'.sys.tables'



					insert into cant_tablas(cant_tablas_bdd1,cant_tablas_bdd2) values
					(@cant_tablas_bdd1,@cant_tablas_bdd2);



					exec sp_executesql @cant_tablas_bdd1;



				commit tran
			end
			------------------------------------------------------------------------------------
		END
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




select * from Esquemas






/*

/**********************************/
--lo siguiente son codigos de pruebas para tenerlos de referencia, hay que sacarlos luego

select DB_ID('comparar2');


select DB_ID('comparar1');




 
 --devuelve el nombre de la columna de una tabla
select COL_NAME(object_id('com2.a'),1)




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
