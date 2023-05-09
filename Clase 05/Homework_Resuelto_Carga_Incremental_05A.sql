
/*### Carga Incremental

En este punto, ya contamos con toda la información de los datasets originales cargada en el DataWarehouse diseñado. 
Sin embargo, la gerencia, requiere la posibilidad de evaluar agregar a esa información la operatoria diara comenzando por la información de Ventas.
 Para lo cual, en la carpeta "Carga_Incremental" se disponibilizaron los archivos:

* Ventas_Actualizado.csv: Tiene una estructura idéntica al original, pero con los registros del día 01/01/2021.
* Clientes_Actializado.csv: Tiena una estructura idéntica al original, pero actualizado al día 01/01/2021.

Es necesario diseñar un proceso que permita ingestar al DataWarehouse las novedades diarias, tanto de la tabla de ventas como de la tabla de clientes.
Se debe tener en cuenta, que dichas fuentes actualizadas, contienen la información original más las novedades, por lo tanto,en la tabla de "ventas" 
es necesario identificar qué registros son nuevos y cuales ya estaban cargados anteriormente, y en la tabla de clientes tambien, con el agregado de que en 
ésta última, pueden haber además registros modificados, con lo que hay que hacer uso de los campos de auditoría de esa tabla, por ejemplo, Fecha_Modificación.

**/


USE henry_m3;
-- buscar que significa buffer:
-- En informática, un "buffer" es una región de la memoria reservada temporalmente
-- para almacenar datos en tránsito mientras se están transfiriendo de un dispositivo o proceso a otro.

DROP TABLE IF EXISTS `venta_novedades`;
CREATE TABLE IF NOT EXISTS `venta_novedades` (
  `IdVenta`				INTEGER,
  `Fecha` 				DATE NOT NULL,
  `Fecha_Entrega` 		DATE NOT NULL,
  `IdCanal`				INTEGER, 
  `IdCliente`			INTEGER, 
  `IdSucursal`			INTEGER,
  `IdEmpleado`			INTEGER,
  `IdProducto`			INTEGER,
  `Precio`				VARCHAR(30),
  `Cantidad`			VARCHAR(30)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Venta_Actualizado.csv' -- pueblo con nuevo doc.
INTO TABLE `venta_novedades` 
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '' 
LINES TERMINATED BY '\n' IGNORE 1 LINES;
-- posee: 46675 registros

SELECT count(*) FROM venta_novedades ORDER BY Fecha desc limit 10; -- sacar el count() para visualizar los primeros 10 ejemplares.


-- lo mismo con clientes
DROP TABLE IF EXISTS cliente_novedades;
CREATE TABLE IF NOT EXISTS cliente_novedades (
	ID					INTEGER,
	Provincia			VARCHAR(50),
	Nombre_y_Apellido	VARCHAR(80),
	Domicilio			VARCHAR(150),
	Telefono			VARCHAR(30),
	Edad				VARCHAR(5),
	Localidad			VARCHAR(80),
	X					VARCHAR(30),
	Y					VARCHAR(30),
    Fecha_Alta			DATE NOT NULL,
    Usuario_Alta		VARCHAR(20),
    Fecha_Ultima_Modificacion		DATE NOT NULL,
    Usuario_Ultima_Modificacion		VARCHAR(20),
    Marca_Baja			TINYINT,
	col10				VARCHAR(1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Clientes_Actualizado.csv'
INTO TABLE cliente_novedades
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';' ENCLOSED BY '\"' ESCAPED BY '\"' 
LINES TERMINATED BY '\n' IGNORE 1 LINES;

SELECT * FROM cliente_novedades Order by ID desc limit 10;

-- ya tengo mis tablas..

/* MUY IMPORTANTE: Se procede primero, a actualizar el Maestro de Clientes, ya que,
 *  debido a que están creadas las restricciones, no sería posible ingestar registros
 *  en la tabla venta que no estén presentes en la tabla cliente*/
        
ALTER TABLE `cliente_novedades` 	ADD `Latitud` DECIMAL(13,10) NOT NULL DEFAULT '0' AFTER `Y`, 
									ADD `Longitud` DECIMAL(13,10) NOT NULL DEFAULT '0' AFTER `Latitud`;
                        
UPDATE cliente_novedades SET Y = '0' WHERE Y = '';
UPDATE cliente_novedades SET X = '0' WHERE X = '';

UPDATE `cliente_novedades` SET Latitud = REPLACE(Y,',','.');
UPDATE `cliente_novedades` SET Longitud = REPLACE(X,',','.');
SELECT * FROM `cliente_novedades` limit 10;


ALTER TABLE `cliente_novedades` DROP `Y`;
ALTER TABLE `cliente_novedades` DROP `X`;

ALTER TABLE `cliente_novedades` DROP `col10`; -- SOBRA ESTA COL.



UPDATE `cliente_novedades` SET Domicilio = 'Sin Dato' WHERE TRIM(Domicilio) = "" OR ISNULL(Domicilio);
UPDATE `cliente_novedades` SET Localidad = 'Sin Dato' WHERE TRIM(Localidad) = "" OR ISNULL(Localidad);
UPDATE `cliente_novedades` SET Nombre_y_Apellido = 'Sin Dato' WHERE TRIM(Nombre_y_Apellido) = "" OR ISNULL(Nombre_y_Apellido);
UPDATE `cliente_novedades` SET Provincia = 'Sin Dato' WHERE TRIM(Provincia) = "" OR ISNULL(Provincia);
-- LOS sin dato.

ALTER TABLE `cliente_novedades` ADD `IdLocalidad` INT NOT NULL DEFAULT '0' AFTER `Localidad`; -- *1

UPDATE cliente_novedades c JOIN aux_localidad a
	ON (c.Provincia = a.Provincia_Original AND c.Localidad = a.Localidad_Original)
SET c.IdLocalidad = a.IdLocalidad;

select * from aux_localidad al  limit 10;
select * from cliente_novedades cn limit 10; 

/*Se chequea que no haya localidades nuevas no detectadas, de ser así, debe ser dada de alta en las tablas respectivas*/
SELECT * FROM cliente_novedades WHERE IdLocalidad = 0; -- se relaciona con *1, si hubiesen nueva localidades se deberia haber cargado con 0.

ALTER TABLE `cliente_novedades`
  DROP `Provincia`,
  DROP `Localidad`;
  
ALTER TABLE `cliente_novedades` ADD `Rango_Etario` VARCHAR(20) NOT NULL DEFAULT '-' AFTER `Edad`;

UPDATE cliente_novedades SET Rango_Etario = '1_Hasta 30 años' WHERE Edad <= 30;
UPDATE cliente_novedades SET Rango_Etario = '2_De 31 a 40 años' WHERE Edad <= 40 AND Rango_Etario = '-';
UPDATE cliente_novedades SET Rango_Etario = '3_De 41 a 50 años' WHERE Edad <= 50 AND Rango_Etario = '-';
UPDATE cliente_novedades SET Rango_Etario = '4_De 51 a 60 años' WHERE Edad <= 60 AND Rango_Etario = '-';
UPDATE cliente_novedades SET Rango_Etario = '5_Desde 60 años' WHERE Edad > 60 AND Rango_Etario = '-';
-- agregue el rango etario.



DROP TABLE IF EXISTS aux_cliente;
CREATE TABLE IF NOT EXISTS aux_cliente (
	IdCliente			INTEGER,
	Latitud				DOUBLE,
	Longitud			DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

INSERT INTO aux_cliente (IdCliente, Latitud, Longitud)
SELECT 	ID, Latitud, Longitud
FROM cliente_novedades WHERE Latitud < -55;

SELECT * FROM aux_cliente;

UPDATE cliente_novedades c JOIN aux_cliente ac
	ON (c.ID = ac.IdCliente)
SET c.Latitud = ac.Longitud, c.Longitud = ac.Latitud;

UPDATE `cliente_novedades` SET Latitud = Latitud * -1 WHERE Latitud > 0;
UPDATE `cliente_novedades` SET Longitud = Longitud * -1 WHERE Longitud > 0;


UPDATE cliente_novedades  SET Latitud = Latitud * -1 WHERE Latitud > 0;
UPDATE cliente_novedades  SET Longitud = Longitud * -1 WHERE Longitud > 0;
-- coreccion de valores. cabio los valores positivos de las coordenadas porque para arg, deben ser si o si neg. por logica

/*
UPDATE cliente_novedades c 
JOIN localidad l ON (c.IdLocalidad = l.IdLocalidad)
SET c.Latitud = l.Latitud
WHERE c.Latitud = 0;

UPDATE cliente_novedades c 
JOIN localidad l ON (c.IdLocalidad = l.IdLocalidad)
SET c.Longitud = l.Longitud
WHERE c.Longitud = 0; 
*/ 
-- desestimado por que estan presentes esas columnas...






/*Validación de Modificaciones:*/
/*(Se puede probar, realizar modificaciones en el archivo Cliente_Actualizado.csv para ver como impactan)*/

SELECT c.*, cn.* 
-- SELECT COUNT(*)
FROM cliente c, cliente_novedades cn
WHERE c.IdCliente = cn.ID -- poco vercoso pero lo dejamos pasar ya que estamos comparando
AND (c.Nombre_Y_Apellido <> cn.Nombre_Y_Apellido OR -- <> significa, distinto de !=
	c.Domicilio <> cn.Domicilio OR
    c.Telefono <> cn.Telefono OR
    c.Edad <> cn.Edad OR
    c.Rango_Etario <> cn.Rango_Etario OR
    c.IdLocalidad <> cn.IdLocalidad OR
    c.Latitud <> cn.Latitud OR
    c.Longitud <> cn.Longitud OR
    c.Fecha_Ultima_Modificacion <> cn.Fecha_Ultima_Modificacion OR
    c.Usuario_Ultima_Modificacion <> cn.Usuario_Ultima_Modificacion OR
    c.Marca_Baja <> cn.Marca_Baja);
-- estoy comparando los clientes que ya estaban con los nuevos, a ver si encuentra diferencias,
--  43 registros dist. encontrados en clase.

-- ahora actualizo donde sean distintos del anterior:
UPDATE cliente c, cliente_novedades cn
SET c.Nombre_Y_Apellido = cn.Nombre_Y_Apellido,
	c.Domicilio = cn.Domicilio,
    c.Telefono = cn.Telefono,
    c.Edad = cn.Edad,
    c.Rango_Etario = cn.Rango_Etario,
    c.IdLocalidad = cn.IdLocalidad,
    c.Latitud = cn.Latitud,
    c.Longitud = cn.Longitud,
    c.Fecha_Ultima_Modificacion = cn.Fecha_Ultima_Modificacion,
    c.Usuario_Ultima_Modificacion = cn.Usuario_Ultima_Modificacion,
    c.Marca_Baja = cn.Marca_Baja
WHERE c.IdCliente = cn.ID
AND (c.Nombre_Y_Apellido <> cn.Nombre_Y_Apellido OR
	c.Domicilio <> cn.Domicilio OR
    c.Telefono <> cn.Telefono OR
    c.Edad <> cn.Edad OR
    c.Rango_Etario <> cn.Rango_Etario OR
    c.IdLocalidad <> cn.IdLocalidad OR
    c.Latitud <> cn.Latitud OR
    c.Longitud <> cn.Longitud OR
    c.Fecha_Ultima_Modificacion <> cn.Fecha_Ultima_Modificacion OR
    c.Usuario_Ultima_Modificacion <> cn.Usuario_Ultima_Modificacion OR
    c.Marca_Baja <> cn.Marca_Baja);
   
   -- 43 filas actualizadas, concuerda con la cantidad de valores != que se encontraron previamente.
 -- a mi me dio 71 rows afected 71 detectadas previamente.
DELETE FROM cliente_novedades cn WHERE cn.ID IN (SELECT c.IdCliente FROM cliente c); -- ahora DELETE las NO novedades

/*Se cargan las novedades en la tabla de Clientes:*/
INSERT INTO cliente (IdCliente, 
					Nombre_Y_Apellido, 
                    Domicilio, 
                    Telefono, 
                    Edad, 
                    Rango_Etario, 
                    IdLocalidad, 
                    Latitud, 
                    Longitud,
					Fecha_Alta,
					Usuario_Alta,
					Fecha_Ultima_Modificacion,
					Usuario_Ultima_Modificacion,
					Marca_Baja)
SELECT	ID, 
		Nombre_Y_Apellido, 
		Domicilio, 
		Telefono, 
		Edad, 
		Rango_Etario, 
		IdLocalidad, 
		Latitud, 
		Longitud,
		Fecha_Alta,
		Usuario_Alta,
		Fecha_Ultima_Modificacion,
		Usuario_Ultima_Modificacion,
		Marca_Baja
FROM 	cliente_novedades;
-- inserto las novedades.



/*Se procede con el procesado de los datos de la tabla venta_novedades
 *  que no hayan sido cargados con anterioridad:*/

DELETE FROM venta_novedades WHERE IdVenta IN (SELECT IdVenta FROM venta);

SELECT * FROM venta_novedades;

UPDATE `venta_novedades` set `Precio` = 0 WHERE `Precio` = '';
ALTER TABLE `venta_novedades` CHANGE `Precio` `Precio` DECIMAL(15,3) NOT NULL DEFAULT '0';

UPDATE venta_novedades v JOIN producto p ON (v.IdProducto = p.IdProducto) 
SET v.Precio = p.Precio
WHERE v.Precio = 0;

UPDATE venta_novedades SET Cantidad = REPLACE(Cantidad, '\r', '');

INSERT INTO aux_venta (IdVenta, Fecha, Fecha_Entrega, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad, Motivo)
SELECT IdVenta, Fecha, Fecha_Entrega, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, 0, 1
FROM venta_novedades WHERE Cantidad = '' or Cantidad is null;

UPDATE venta_novedades SET Cantidad = '1' WHERE Cantidad = '' or Cantidad is null;
ALTER TABLE `venta_novedades` CHANGE `Cantidad` `Cantidad` INTEGER NOT NULL DEFAULT '0';

-- OUTLAIERS:
INSERT INTO aux_venta (IdVenta, Fecha, Fecha_Entrega, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad, Motivo)
SELECT v.IdVenta, v.Fecha, v.Fecha_Entrega, v.IdCliente, v.IdSucursal, v.IdEmpleado, v.IdProducto, v.Precio, v.Cantidad, 2
FROM venta_novedades v 
JOIN (SELECT IdProducto, AVG(Cantidad) As Promedio, STDDEV(Cantidad) as Desv FROM venta_novedades GROUP BY IdProducto) v2
	on (v.IdProducto = v2.IdProducto)
WHERE v.Cantidad > (v2.Promedio + (3 * v2.Desv)) OR v.Cantidad < 0 OR v.Cantidad < (v2.Promedio - (3 * v2.Desv));

INSERT INTO aux_venta (IdVenta, Fecha, Fecha_Entrega, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad, Motivo)
SELECT v.IdVenta, v.Fecha, v.Fecha_Entrega, v.IdCliente, v.IdSucursal, v.IdEmpleado, v.IdProducto, v.Precio, v.Cantidad, 3
FROM venta_novedades v 
JOIN (SELECT IdProducto, AVG(Precio) As Promedio, STDDEV(Precio) as Desv FROM venta_novedades GROUP BY IdProducto) v2
	on (v.IdProducto = v2.IdProducto)
WHERE v.Precio > (v2.Promedio + (3 * v2.Desv)) OR v.Precio < 0 OR v.Precio < (v2.Promedio - (3 * v2.Desv));

select * from aux_venta where Motivo = 2; -- outliers de cantidad
select * from aux_venta where Motivo = 3; -- outliers de precio

ALTER TABLE `venta_novedades` ADD `Outlier` TINYINT NOT NULL DEFAULT '1' AFTER `Cantidad`;

UPDATE venta_novedades v JOIN aux_venta a
	ON (v.IdVenta = a.IdVenta AND a.Motivo IN (2,3))
SET v.Outlier = 0;

UPDATE venta_novedades SET IdEmpleado = (IdSucursal * 1000000) + IdEmpleado;

/*Finalmente.. se cargan las novedades en la tabla de Ventas:*/
INSERT INTO venta (IdVenta, Fecha, Fecha_Entrega, IdCanal, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad, Outlier)
SELECT IdVenta, Fecha, Fecha_Entrega, IdCanal, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad, Outlier
FROM venta_novedades;

-- EN RESUMEN CREAMOS TABLAS IDENTICAS COMO BUFFER Y COMPARO luego de unos cambios poblamos la tabla a gusto podria decirse
/*
 * En esencia el Buffer pool se encarga de guardar en memoria nuestros datos de una tabla concreta y sus índices en caché, para que,
 * si en algún momento se vuelven a requerir esos datos, cargarlos desde la memoria en lugar del disco, como ya sabéis, un disco es mucho 
 * más lento que una memoría RAM.
 */