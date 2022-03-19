
CREATE TABLE IF NOT EXISTS datos_ar (
	cod_localidad INT NOT NULL,
	id_provincia INT NOT NULL,
	id_departamento INT NOT NULL,
	categoría VARCHAR(255),
	provincia VARCHAR(255),
	localidad VARCHAR(255),
	nombre VARCHAR(255),
	domicilio VARCHAR(255),
	código_postal VARCHAR(255),
	mail VARCHAR(255),
	número_de_teléfono VARCHAR(255),
	web VARCHAR(255),
	fecha_de_carga DATE

);
