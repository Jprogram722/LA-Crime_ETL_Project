-- create a stored procedure so that I can execute this more easily

CREATE PROCEDURE merge_data()
LANGUAGE SQL
AS $$

	-- for the weapon table
	MERGE INTO weapon AS w
	USING weapon_tmp AS w_tmp
	ON w.weapon_id_pk = w_tmp.weapon_id_pk
	WHEN MATCHED THEN
		UPDATE SET weapon = w_tmp.weapon
	WHEN NOT MATCHED THEN
		INSERT (weapon_id_pk, weapon)
		VALUES (w_tmp.weapon_id_pk, w_tmp.weapon);
	
	
	-- for the area table
	MERGE INTO area AS a
	USING area_tmp AS a_tmp
	ON a.area_id_pk = a_tmp.area_id_pk
	WHEN MATCHED THEN
		UPDATE SET area_name = a_tmp.area_name
	WHEN NOT MATCHED THEN
		INSERT (area_id_pk, area_name)
		VALUES (a_tmp.area_id_pk, a_tmp.area_name);
	
	-- for the location table
	MERGE INTO location AS l
	USING location_tmp AS l_tmp
	ON l.location_id_pk = l_tmp.location_id_pk
	WHEN MATCHED THEN
		UPDATE SET location_name = l_tmp.location_name
	WHEN NOT MATCHED THEN
		INSERT (location_id_pk, location_name)
		VALUES (l_tmp.location_id_pk, l_tmp.location_name);
	
	-- for the premisis table
	MERGE INTO premisis AS p
	USING premisis_tmp AS p_tmp
	ON p.premisis_id_pk = p_tmp.premisis_id_pk
	WHEN MATCHED THEN
		UPDATE SET premisis_desc = p_tmp.premisis_desc
	WHEN NOT MATCHED THEN
		INSERT (premisis_id_pk, premisis_desc)
		VALUES (p_tmp.premisis_id_pk, p_tmp.premisis_desc);
	
	-- for the status table
	MERGE INTO status AS s
	USING status_tmp AS s_tmp
	ON s.status_code = s_tmp.status_code
	WHEN MATCHED THEN
		UPDATE SET status_desc = s_tmp.status_desc
	WHEN NOT MATCHED THEN
		INSERT (status_code, status_desc)
		VALUES (s_tmp.status_code, s_tmp.status_desc);
	
	-- for the crime table
	MERGE INTO crime AS c
	USING crime_tmp AS c_tmp
	ON c.crime_id_pk = c_tmp.crime_id_pk
	WHEN MATCHED THEN
		UPDATE SET crime_desc = c_tmp.crime_desc
	WHEN NOT MATCHED THEN
		INSERT (crime_id_pk, crime_desc)
		VALUES (c_tmp.crime_id_pk, c_tmp.crime_desc);
	
	-- for the report data
	MERGE INTO report AS r
	USING report_tmp AS r_tmp
	ON r.report_id_pk = r_tmp.report_id_pk
	WHEN MATCHED THEN
		UPDATE SET date_reported = r_tmp.date_reported,
		date_occured = r_tmp.date_occured,
		victim_age = r_tmp.victim_age,
		victim_sex = r_tmp.victim_sex,
		victim_decent = r_tmp.victim_decent,
		area_id_fk = r_tmp.area_id_fk,
		premisis_id_fk = r_tmp.premisis_id_fk,
		weapon_id_fk = r_tmp.weapon_id_fk,
		location_id_fk = r_tmp.location_id_fk,
		status_code = r_tmp.status_code,
		latitude = r_tmp.latitude,
		longitude = r_tmp.longitude
	WHEN NOT MATCHED THEN
		INSERT (report_id_pk,date_reported, date_occured, victim_age, victim_sex, victim_decent, 
		area_id_fk, premisis_id_fk, weapon_id_fk, location_id_fk, status_code,
		latitude,longitude)
		VALUES
		(r_tmp.report_id_pk, r_tmp.date_reported, r_tmp.date_occured, r_tmp.victim_age, r_tmp.victim_sex, r_tmp.victim_decent, 
		r_tmp.area_id_fk, r_tmp.premisis_id_fk, r_tmp.weapon_id_fk, r_tmp.location_id_fk, r_tmp.status_code,
		r_tmp.latitude, r_tmp.longitude);
	
	-- for the crime_report table
	-- these values shouldn't change since they are PK's of other tables
	-- also they have update cascade already
	MERGE INTO crime_report AS cr
	USING crime_report_tmp AS cr_tmp
	ON cr.report_id_fk = cr_tmp.report_id_fk
	WHEN NOT MATCHED THEN
		INSERT (report_id_fk, crime_id_fk)
		VALUES (cr_tmp.report_id_fk, cr_tmp.crime_id_fk);
$$