CREATE TABLE IF NOT EXISTS area (
	area_id_pk INT PRIMARY KEY,
	area_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS premisis(
	premisis_id_pk INT PRIMARY KEY,
	premisis_desc VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS weapon(
	weapon_id_pk INT PRIMARY KEY,
	weapon VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS location(
	location_id_pk INT PRIMARY KEY,
	location_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS status(
	status_code CHAR(2) PRIMARY KEY,
	status_desc VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS report(
	report_id_pk INT PRIMARY KEY,
	date_reported DATE,
	date_occured TIMESTAMP,
	victim_age INT,
	victim_sex CHAR(2),
	victim_decent CHAR(2),
	area_id_fk INT REFERENCES area (area_id_pk) ON DELETE CASCADE ON UPDATE CASCADE,
	premisis_id_fk INT REFERENCES premisis (premisis_id_pk) ON DELETE CASCADE ON UPDATE CASCADE,
	weapon_id_fk INT REFERENCES weapon (weapon_id_pk) ON DELETE CASCADE ON UPDATE CASCADE,
	location_id_fk INT REFERENCES location (location_id_pk) ON DELETE CASCADE ON UPDATE CASCADE,
	status_code CHAR(2) REFERENCES status (status_code) ON DELETE CASCADE ON UPDATE CASCADE,
	latitude FLOAT,
	longitude FLOAT
);

CREATE TABLE IF NOT EXISTS crime(
	crime_id_pk INT PRIMARY KEY,
	crime_desc VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS crime_report(
	crime_report_id_pk SERIAL PRIMARY KEY,
	report_id_fk INT REFERENCES report (report_id_pk) ON DELETE CASCADE ON UPDATE CASCADE,
	crime_id_fk INT REFERENCES crime (crime_id_pk) ON DELETE CASCADE ON UPDATE CASCADE
);