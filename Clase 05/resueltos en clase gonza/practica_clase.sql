SET @anio = 2019;
SELECT @anio;
SELECT *
FROM fact_venta
WHERE year(Fecha) = @anio;

SELECT @test := IdVenta FROM venta WHERE IdVenta = 25;
SELECT @test;

SHOW VARIABLES;

-- Crear y trabajar con funciones
DELIMITER $$
CREATE FUNCTION antMeses(fechaIngreso DATE) RETURNS INT
BEGIN
	DECLARE meses INT;
    SET meses = timestampdiff(month, fechaIngreso, curdate());
    RETURN meses;
END$$
DELIMITER ;
SET @resul_meses = antMeses('2018-05-05');
SELECT @resul_meses;
SET GLOBAL log_bin_trust_function_creators = 1; -- Permiso para crear funciones

-- Crear y trabajar con procedimientos
use henry;
DROP PROCEDURE getTotalAlumnos; 
DELIMITER $$
CREATE PROCEDURE getTotalAlumnos()
BEGIN
	DECLARE total_alumnos INT DEFAULT 0;
	SELECT count(*)
    INTO total_alumnos
    FROM alumno;
    SELECT total_alumnos;
END$$
DELIMITER ;

CALL getTotalAlumnos();

