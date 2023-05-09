/*
 ## Homework

1. Crear una tabla que permita realizar el seguimiento de los usuarios que ingresan nuevos registros en fact_venta.
2. Crear una acción que permita la carga de datos en la tabla anterior.
3. Crear una tabla que permita registrar la cantidad total registros, luego de cada ingreso la tabla fact_venta.
4. Crear una acción que permita la carga de datos en la tabla anterior.
5. Crear una tabla que agrupe los datos de la tabla del item 3, a su vez crear un proceso de carga de los datos agrupados.
6. Crear una tabla que permita realizar el seguimiento de la actualización de registros de la tabla fact_venta.
7. Crear una acción que permita la carga de datos en la tabla anterior, para su actualización.

### Carga Incremental

En este punto, ya contamos con toda la información de los datasets originales cargada en el DataWarehouse diseñado. 
Sin embargo, la gerencia, requiere la posibilidad de evaluar agregar a esa información la operatoria diara comenzando por la información de Ventas.
 Para lo cual, en la carpeta "Carga_Incremental" se disponibilizaron los archivos:

* Ventas_Actualizado.csv: Tiene una estructura idéntica al original, pero con los registros del día 01/01/2021.
* Clientes_Actializado.csv: Tiena una estructura idéntica al original, pero actualizado al día 01/01/2021.

Es necesario diseñar un proceso que permita ingestar al DataWarehouse las novedades diarias, tanto de la tabla de ventas como de la tabla de clientes.
Se debe tener en cuenta, que dichas fuentes actualizadas, contienen la información original más las novedades, por lo tanto,en la tabla de "ventas" 
es necesario identificar qué registros son nuevos y cuales ya estaban cargados anteriormente, y en la tabla de clientes tambien, con el agregado de que en 
ésta última, pueden haber además registros modificados, con lo que hay que hacer uso de los campos de auditoría de esa tabla, por ejemplo, Fecha_Modificación.
 */



use henry_m3;
-- 1. Crear una tabla que permita realizar el seguimiento de los usuarios que ingresan nuevos registros en fact_venta.

-- 1) Creamos la tabla que auditará a los usuarios que realizan cambios
DROP TABLE IF EXISTS `fact_venta_auditoria`;
CREATE TABLE IF NOT EXISTS `fact_venta_auditoria` (
	`Fecha`				DATE,
	`Fecha_Entrega`		DATE,
  	`IdCanal` 			INTEGER,
  	`IdCliente` 		INTEGER,
  	`IdEmpleado` 		INTEGER,
  	`IdProducto` 		INTEGER,
    `usuario` 			VARCHAR(20),
    `fechaModificacion` 	DATETIME
);
select * from fact_venta_auditoria fva;



-- 1.2 Creamos el trigger que se ejecutara luego de cada cambio.
-- un trigger es un tipo de programa que se ejecuta automáticamente
-- cuando se cumple cierta condición en una tabla.

DROP TRIGGER fact_venta_auditoria;
CREATE TRIGGER fact_venta_auditoria AFTER INSERT ON fact_venta -- crea un trigger para que SIEMPRE QUE SE INSERTE un valor en fact_venta
FOR EACH ROW -- Para cada linea inserta...
INSERT INTO fact_venta_auditoria (Fecha, Fecha_Entrega, IdCanal, IdCliente, IdEmpleado, IdProducto, usuario, fechaModificacion) -- INSERTAME DENTRO de fact_venta_auditoria, columnas(Fecha, Fecha_Entrega, IdCanal, IdCliente, IdEmpleado, IdProducto, usuario, fechaModificacion)
VALUES (NEW.Fecha, NEW.Fecha_Entrega, NEW.IdCanal, NEW.IdCliente, NEW.IdEmpleado, NEW.IdProducto, CURRENT_USER,NOW()); -- los VALORES(NUEVO.Fecha, NUEVO.Fecha_Entrega, NUEVO....)
-- current user, el user que realiza la modificacion, new apunta al nuevo valor agregado
-- select CURRENT_USER,NOW(); -- current user example:

truncate table fact_venta; -- vacía la tabla, pero no la borra.
truncate table fact_venta_auditoria;

select * from fact_venta;
select * from fact_venta_auditoria;


-- 2) 2. Crear una acción que permita la carga de datos en la tabla anterior.

insert into fact_venta (IdVenta, Fecha, Fecha_Entrega, IdCanal, IdCliente, IdEmpleado, IdProducto, Precio, Cantidad)
select IdVenta, Fecha, Fecha_Entrega, IdCanal, IdCliente, IdEmpleado, IdProducto, Precio, Cantidad
from venta;
select * from fact_venta fv limit 10;
select count(*) from fact_venta; -- verifico
select count(*) from fact_venta_auditoria; -- verifico

select * from fact_venta_auditoria limit 10; -- puedo ver en la tabla auditoria quien insertó los valores



-- 3)3. Crear una tabla que permita registrar la cantidad total de registros, 
-- luego de cada ingreso la tabla fact_venta. 

-- Creamos la tabla que llevara una cuenta de los registros.
DROP TABLE IF EXISTS `fact_venta_registros`;
CREATE TABLE IF NOT EXISTS `fact_venta_registros` (
  	id 	INT NOT NULL AUTO_INCREMENT,
	cantidadRegistros INT,
	usuario VARCHAR (20),
	fecha DATETIME,
	PRIMARY KEY (id)
);

select * from fact_venta_registros limit 10;


-- 4)4. Crear una acción que permita la carga de datos en la tabla anterior.
--  Creamos el trigger que se ejecutara luego de cada cambio. 
-- (va a tardar un poco mas debido a que ahora tengo dos triggers)

DROP TRIGGER fact_venta_registros; -- segundo TRIGGER creado.
CREATE TRIGGER fact_venta_registros AFTER INSERT ON fact_venta
FOR EACH ROW
INSERT INTO fact_inicial_registros (cantidadRegistros,usuario, fecha)
VALUES ((SELECT COUNT(*) FROM fact_venta),CURRENT_USER,NOW());




-- 5)5. Crear una tabla que agrupe los datos de la tabla del item 3, a su vez crear un proceso de carga de los datos agrupados. 
-- Creamos una tabla donde podremos almacenar la cantidad de registros por día
DROP TABLE registros_tablas;
CREATE TABLE registros_tablas (
id INT NOT NULL AUTO_INCREMENT,
tabla VARCHAR(30),
fecha_hora DATETIME,
cantidadRegistros INT,
PRIMARY KEY (id)
);

-- Esta instrucción nos permite cargar la tabla anterior y saber cual es la cantidad de registros por día.
INSERT INTO registros_tablas (tabla, fecha_hora, cantidadRegistros)-- mete dentro registros tablas, 
SELECT 'venta', Now(), COUNT(*) FROM venta; -- el resultado de count(*) de la tabla ventas.
INSERT INTO registros_tablas (tabla, fecha_hora, cantidadRegistros) -- lo mismo para gasto.
SELECT 'gasto', Now(), COUNT(*) FROM gasto;
INSERT INTO registros_tablas (tabla, fecha_hora, cantidadRegistros)-- lo mismo para compra.
SELECT 'compra', Now(), COUNT(*) FROM compra;

SELECT * FROM registros_tablas;
show triggers; -- muestra los triggers creados.
-- show triggers; -- muestra los triggers creados en la database donde estoy con el use, asi mismo, show tables, muestra todas as tablas de la database.

SELECT DATE('2011-01-01 00:00:10');






-- 6)6. Crear una tabla que permita realizar el seguimiento de la actualización de registros de la tabla fact_venta.
-- Creamos una tabla para auditar cambios
DROP TABLE IF EXISTS `fact_venta_cambios`;
CREATE TABLE IF NOT EXISTS `fact_venta_cambios` (
  	`Fecha` 			DATE,
  	`IdCliente` 		INTEGER,
  	`IdProducto` 		INTEGER,
    `Precio` 			DECIMAL(15,3),
    `Cantidad` 			INTEGER,
    `usuario` 			VARCHAR(20),
    `fechaModificacion` DATETIME
);

-- Creamos el trigger que carga nuevos registros
DROP TRIGGER auditoria_cambios;
CREATE TRIGGER auditoria_cambios AFTER UPDATE ON fact_venta -- AFTER UPDATE, luego de actualizar...
FOR EACH ROW
INSERT INTO fact_venta_cambios (Fecha, IdCliente, IdProducto, Precio, Cantidad, usuario,fechaModificacion)
VALUES (OLD.Fecha,OLD.IdCliente, OLD.IdProducto, OLD.Precio, OLD.Cantidad, CURRENT_USER, NOW());
-- OLD. quiero ver el valor viejo, el que tenia antes del update. para supervisar el valor que estaba
-- previo a la actualizacion.
-- para poder monitorear el valor que se cambió, el OLD.value.



-- 7)7. Crear una acción que permita la carga de datos en la tabla anterior, para su actualización.

-- solucion gonza:

SELECT * FROM fact_venta_cambios;
SELECT * FROM fact_venta;
/*UPDATE fact_venta
SET Precio = xx -- xx sera el valor a sustituir
WHERE IdVenta = xx; -- donde el id de venta sea...
*/

-- y puedo ver en la tabla fact_venta_cambios que el valor nuevo no es igual al nuevo valor, se editó el campo.
-- puedo identificar, quien hizo el cambio, donde y que cambio..



/* -- solucion que venia en el resuelto:
SELECT * FROM fact_venta_cambios;
select * from fact_venta where IdVenta = 1;
update fact_venta set Precio = 820 where IdVenta = 1;

-- Variante para los puntos 6 y 7
-- Creamos el trigger que carga cambios en los registros
DROP TRIGGER auditoria_actualizacion;
CREATE TRIGGER auditoria_actualizacion AFTER UPDATE ON fact_venta
FOR EACH ROW
UPDATE fact_venta_cambios
SET 
IdCliente = OLD.IdCliente, 
IdProducto = OLD.IdProducto,
IdCliente1 = NEW.IdCliente, 
IdProducto1 = NEW.IdProducto
WHERE Fecha = OLD.Fecha;
*/