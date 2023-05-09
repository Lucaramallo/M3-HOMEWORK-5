use henry;

CREATE TABLE auditoria_alumno (
	idAuditoria INT NOT NULL AUTO_INCREMENT,
    descripcion VARCHAR(50) NOT NULL,
    fecha_y_hora DATETIME NOT NULL,
    usuario VARCHAR(50),
    PRIMARY KEY (idAuditoria)
);

DROP TRIGGER audit_alumno;
CREATE TRIGGER audit_alumno AFTER INSERT ON alumno
FOR EACH ROW
INSERT INTO auditoria_alumno (descripcion, fecha_y_hora, usuario)
VALUES (concat('Nueva insersión: ', NEW.idAlumno, NEW.nombre, NEW.apellido), now(), current_user());

