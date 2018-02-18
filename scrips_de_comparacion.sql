create database comparaciones;

use comparaciones;



/************************/
--ver si existen las bdd a comparar
go
create procedure ComprobarQueExistanBDD @bdd1 varchar(max),@bdd2 varchar(max)
as
begin
set nocount on

	begin try
		--veo si existen las bdd, y mansa un mensaje dependiendo cual no esta
		if db_id(@bdd1) is null and  db_id(@bdd2) is not null raiserror('NO SE ENCUENTRA LA BDD1',16,1)	
		if db_id(@bdd2) is null and  db_id(@bdd1) is not null raiserror('NO SE ENCUENTRA LA BDD2',16,1)
		if db_id(@bdd2) is  null and  db_id(@bdd1) is  null raiserror('NO SE ENCUENTRAN NINGUNA BDD',16,1)

			
			--bloque que compara el nombre de los esquemas
			begin		
				declare @esquema_bdd1 nvarchar(max);
				--lo siguiente es sql dinamico
				set @esquema_bdd1='select name as esquema_bdd1_queNoEstaEnLa2
								   from '+@bdd1+'.sys.schemas
								   where '+@bdd1+'.sys.schemas.name not in (select name
																			from '+@bdd2+'.sys.schemas);';

			
				declare @esquema_bdd2 nvarchar(max);
					set @esquema_bdd2='select name as esquema_bdd2_queNoEstaEnLa1
									   from '+@bdd2+'.sys.schemas
									   where '+@bdd2+'.sys.schemas.name not in (select name
																				from '+@bdd1+'.sys.schemas);';
					

				--ejecuto los declare de arriba para que muestren el resultado de la comparacion
				EXECUTE SP_EXECUTESQL @esquema_bdd1;
				EXECUTE SP_EXECUTESQL @esquema_bdd2;
				
			
			end 
			

			--transaccion para comparar tablas por nombre
			begin 

				declare @nombre_campoBdd1 nvarchar(max)
				declare @nombre_campoBdd2 nvarchar(max)

				set @nombre_campoBdd1='select name as tablas_bdd1_queNoEstanEnLa2
								   from '+@bdd1+'.sys.tables
								   where '+@bdd1+'.sys.tables.name not in (select name
																			from '+@bdd2+'.sys.tables);';


				
				set @nombre_campoBdd2='select name as tablas_bdd2_queNoEstanEnLa1
								   from '+@bdd2+'.sys.tables
								   where '+@bdd2+'.sys.tables.name not in (select name
																			from '+@bdd1+'.sys.tables);';

			EXECUTE SP_EXECUTESQL @nombre_campoBdd1;
			EXECUTE SP_EXECUTESQL @nombre_campoBdd2;
			end 

	end try


	begin catch

		--declaro las variables de rror, para pasarlo por el raiseror
		--en teoria ya puse el mensaje en el begin try, 
		--esique nose bien el porque de esto,pero bue :P
		DECLARE @ErrorMessage NVARCHAR(4000);
		DECLARE @ErrorSeverity INT;
		DECLARE @ErrorState INT;
		 SELECT 
			@ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE();
			 RAISERROR (@ErrorMessage,
				   @ErrorSeverity,
				   @ErrorState 
				   );
	end catch
end

--ejecuto el procedimiento
exec ComprobarQueExistanBDD 'comparar1','comparar2';

-----------------------------







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



	
select name 
from comparar1.sys.schemas
	
select name 
from comparar2.sys.schemas



					--traigo el nombre de todas las tablas de una bdd
					select name from sys.tables

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








