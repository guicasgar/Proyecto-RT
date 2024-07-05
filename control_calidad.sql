-- comprobacion secuencias
select id_recorrido, num_secuencia, num_secuencia_esperada from
	(SELECT id_recorrido, num_secuencia,
			   ROW_NUMBER() OVER (PARTITION BY id_carretera, agrupacion, sentido, enlace, tipo_recorrido 
								  ORDER BY id_carretera, tipo_recorrido, enlace, sentido, num_secuencia) AS num_secuencia_esperada
	FROM recorrido
	ORDER BY id_carretera, tipo_recorrido, enlace, sentido, num_secuencia) as subconsulta
where num_secuencia <> num_secuencia_esperada

-- comprobacion_rutas
SELECT 
    REPLACE(
        CONCAT(
            '\ALMERIA\', r.agrupacion, '\',
            CASE 
                WHEN r.tipo_recorrido = 1 THEN c.codigo || '\' || c.codigo || '_' || r.sentido || '_'
                WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'
                WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'
                ELSE ''
            END,
            TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', 
            LPAD(r.num_secuencia::text, 2, '0'), '\P\', v.nombre, '.m4v'
        ) || ';',
        ';',
        E'\n'
    ) AS resultado_total
FROM 
    recorrido_almeria r
JOIN 
    carretera_almeria c ON r.id_carretera = c.id_carretera
JOIN 
    video_almeria v ON r.id_recorrido = v.id_recorrido

UNION ALL

SELECT 
    REPLACE(
        CONCAT(
            '\ALMERIA\', r.agrupacion, '\',
            CASE 
                WHEN r.tipo_recorrido = 1 THEN c.codigo || '\' || c.codigo || '_' || r.sentido || '_'
                WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'
                WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'
                ELSE ''
            END,
            TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', 
            LPAD(r.num_secuencia::text, 2, '0'), '\M\', v.nombre, '.m4v'
        ) || ';',
        ';',
        E'\n'
    ) AS resultado_total
FROM 
    recorrido_almeria r
JOIN 
    carretera_almeria c ON r.id_carretera = c.id_carretera
JOIN 
    video_almeria v ON r.id_recorrido = v.id_recorrido

UNION ALL

SELECT 
    REPLACE(
        CONCAT(
            '\ALMERIA\', r.agrupacion, '\',
            CASE 
                WHEN r.tipo_recorrido = 1 THEN c.codigo || '\' || c.codigo || '_' || r.sentido || '_'
                WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'
                WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'
                ELSE ''
            END,
            TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', 
            LPAD(r.num_secuencia::text, 2, '0'), '\G\', v.nombre, '.m4v'
        ) || ';',
        ';',
        E'\n'
    ) AS resultado_total
FROM 
    recorrido_almeria r
JOIN 
    carretera_almeria c ON r.id_carretera = c.id_carretera
JOIN 
    video_almeria v ON r.id_recorrido = v.id_recorrido

UNION ALL

SELECT 
    REPLACE(
        CONCAT(
            '\ALMERIA\', r.agrupacion, '\',
            CASE 
                WHEN r.tipo_recorrido = 1 THEN c.codigo || '\' || c.codigo || '_' || r.sentido || '_'
                WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'
                WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'
                ELSE ''
            END,
            TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', 
            LPAD(r.num_secuencia::text, 2, '0'), '\ORIGINAL\', v.nombre, '.m4v'
        ) || ';',
        ';',
        E'\n'
    ) AS resultado_total
FROM 
    recorrido_almeria r
JOIN 
    carretera_almeria c ON r.id_carretera = c.id_carretera
JOIN 
    video_almeria v ON r.id_recorrido = v.id_recorrido;

-- Consulta si el gpkg tiene las mismas rutas que la lista_ruta_video
select rutas 
from gpkg_almeria
where rutas not in (select rutas from lista_rutas_video_almeria);

-- Tabla_recorrido, sentido, el mismo que sentido tabla tramo_carretera
select 
	tra.id_tramo, 
	ra.sentido sentido_recorrido, 
	tca.sentido sentido_tramo_carretera		
from recorrido_almeria ra
join tramo_recorrido_almeria tra on tra.id_recorrido = ra.id_recorrido
join tramo_carretera_almeria tca on tca.id_tramo = tra.id_tramo
where (ra.sentido = '1' or ra.sentido='2') and (tca.sentido = '1' or tca.sentido='2') and ra.sentido <> tca.sentido and tra.recorrido_adyacente=false
order by tra.id_tramo;

-- tabla tramo, tipo_tramo, tiene que ser el mismo que tipo_recorrido de la tabla recorrido, o bien,  
-- se comprueba aquellos recorridos que deberian llevar recorrido_adyacente como verdadero
SELECT 
    tra.id_tramo,
    sub.tipo_tramo_transformado AS tipo_tramo,
    ra.tipo_recorrido,
	tra.recorrido_adyacente
FROM 
    tramo_recorrido_almeria tra
JOIN 
    (SELECT 
        ta.id_tramo,
        ta.tipo_tramo,
        CASE
            WHEN ta.tipo_tramo IN (4, 21, 22, 23, 24, 25, 26, 51, 52, 53, 91, 99) THEN 2
            WHEN ta.tipo_tramo IN (31, 32, 33) THEN 3
            ELSE ta.tipo_tramo
        END AS tipo_tramo_transformado
    FROM 
        tramo_almeria ta) AS sub
    ON tra.id_tramo = sub.id_tramo
JOIN 
    recorrido_almeria ra ON tra.id_recorrido = ra.id_recorrido
WHERE
    sub.tipo_tramo_transformado <> ra.tipo_recorrido and tra.recorrido_adyacente=false
ORDER BY 
    tra.id_tramo;

-- Control de solapes entre videos.
SELECT a.id, b.id
FROM video_almeria a, video_almeria b
WHERE a.id <> b.id
  AND ST_Overlaps(a.geom, b.geom);


-- Comprueba que no haya fotogramas duplicados entre 'fotograma' y 'next_fotograma', así como la distancia entre ellos. 
-- Cuando la distancia es mayor de 12 y menor de 2 se consideran falsos positivos
WITH ordered_fotogramas AS (
    SELECT
        id_video,
        fotograma,
        geom,
        LEAD(geom) OVER (PARTITION BY id_video ORDER BY fotograma) AS next_geom,
        LEAD(fotograma) OVER (PARTITION BY id_video ORDER BY fotograma) AS next_fotograma
    FROM
        fotograma_almeria
),
distances AS (
    SELECT
        id_video,
        fotograma,
        next_fotograma,
        round(ST_Distance(ST_Transform(geom, 32633), ST_Transform(next_geom, 32633))::numeric,2) AS distance
    FROM
        ordered_fotogramas
    WHERE
        next_geom IS NOT NULL
)
SELECT
    id_video,
    fotograma,
    next_fotograma,
    distance
FROM
    distances
WHERE
    distance < 2 or distance > 12
order by distance desc;

--- Que todos los tramos de la tabla tramo aparecen en la tabla "tramo_recorrido"
select ta.id_tramo, 'Este no aparece' AS mensaje
from tramo_almeria ta
where ta.id_tramo not in(select tra.id_tramo from tramo_recorrido_almeria tra)
order by ta.id_tramo;

-- Que todos los recorridos de la tabla "recorrido" aparecen en la tabla "tramo_recorrido"
   -- si hay más recorridos de los que aparecen en tramo_recorrido "sobrarían videos"
   -- si hay menos recorridos de los que aparecen en tramo_recorrido "faltarían videos" o faltaría asignar el recorrido a ese tramo
SELECT 
    ra.id_recorrido AS id_recorrido_de_la_tabla_recorrido_almeria, 
    NULL AS id_recorrido_de_la_tabla_tramo_recorrido_almeria,
    CASE 
        WHEN ra.id_recorrido IS NOT NULL AND tra.id_recorrido IS NULL THEN 'sobran vídeos o error de asignación' 
        ELSE '' 
    END AS mensaje
FROM 
    recorrido_almeria ra
LEFT JOIN 
    (SELECT tra.id_recorrido FROM tramo_recorrido_almeria tra) tra
ON 
    ra.id_recorrido = tra.id_recorrido
WHERE 
    tra.id_recorrido IS NULL

UNION

SELECT 
    NULL AS id_recorrido_almeria, 
    tra.id_recorrido AS id_recorrido_tramo_almeria,
    CASE 
        WHEN tra.id_recorrido IS NOT NULL AND ra.id_recorrido IS NULL THEN 'falta vídeos o error de asignación' 
        ELSE '' 
    END AS mensaje
FROM 
    tramo_recorrido_almeria tra
LEFT JOIN 
    (SELECT ra.id_recorrido FROM recorrido_almeria ra) ra
ON 
    tra.id_recorrido = ra.id_recorrido
WHERE 
    ra.id_recorrido IS NULL;

-- Que todos los recorridos de la tabla "recorrido" aparezcan en la tabla "video"
SELECT 
    ra.id_recorrido,
    'Este no aparece' AS mensaje
FROM 
    recorrido_almeria ra
WHERE 
    ra.id_recorrido NOT IN (SELECT id_recorrido FROM video_almeria)
ORDER BY 
    ra.id_recorrido;

-- Que todos los videos tienen fotogramas (se comprueba por id_video)
select va.id_video, 'Este no aparece' AS mensaje
from video_almeria va
where va.id_video not in(select fa.id_video f from fotograma_almeria fa)
order by va.id_video;


-- Que todos los recorridos con tipo_recorrido 1 solo pueden tener sentido 1 o 2. Y los tipo_recorrido =2 o 3 solo pueden tener sentido -998
select * from
(select ra.sentido, ra.tipo_recorrido,
	case when ra.tipo_recorrido='1' then 
			case when ra.sentido = '1' or ra.sentido= '2' then 'Correcto'
				 else 'Incorrecto'
			end
		 when ra.tipo_recorrido='2' or ra.tipo_recorrido='3' then 
		 	case when ra.sentido = '-998'then 'Correcto'
				 else 'Incorrecto'
			end
		 else 'Incorrecto'
	end as validacion	
from recorrido_almeria ra) as sub
where validacion = 'Incorrecto' or validacion = 'Correcto'

-- Que no haya nulos en los siguientes campos de las siguientes tablas:
     -- "tramo_recorrido": id ; id_recorrido ; id_tramo ; recorrido_adyacente
     -- "video": fecha_grabacion ; id_camara ; id_recorrido ; id_video ; lote  ; nombre
     -- "recorrido": agrupacion ; fecha_grabacion ; id_carretera ; id_recorrido ; num_secuencia ; sentido ; tipo_recorrido
     -- "fotograma" : fotograma ; id_fotograma ; id_video ; orientacion ; tiempo
	 
SELECT EXISTS (
    SELECT 1
    FROM tramo_recorrido_almeria
    WHERE id IS NULL 
       OR id_tramo is NULL
       OR id_recorrido IS NULL
	   OR recorrido_adyacente IS NULL
) AS existen_nulls_en_tramo_recorrido_almeria

SELECT EXISTS (
    SELECT 1
    FROM video_almeria
    WHERE id_video IS NULL 
	   OR id_recorrido is NULL
	   OR id_camara IS NULL
	   OR lote IS NULL
		OR nombre IS NULL
		OR fecha_grabacion IS NULL
) AS existen_nulls_en_video_almeria

SELECT *
FROM recorrido_almeria
WHERE id_recorrido IS NULL 
   OR id_carretera IS NULL
   OR agrupacion IS NULL
   OR sentido IS NULL
   OR tipo_recorrido IS NULL
   OR num_secuencia IS NULL
   OR fecha_grabacion IS NULL
   OR ((tipo_recorrido = 2 OR tipo_recorrido = 3) AND (enlace IS NULL OR enlace = ''))
   OR (tipo_recorrido = 1 AND (enlace IS NOT NULL AND enlace != ''));

SELECT EXISTS (
    SELECT 1
    FROM fotograma_almeria
    WHERE id_fotograma IS NULL 
	   OR id_video is NULL
	   OR fotograma IS NULL
	   OR tiempo IS NULL
	   OR orientacion IS NULL
) AS existen_nulls_en_fotograma_almeria


-- Los tramos troncales no pueden tener recorrido_adyacente verdadero.
select ta.id_tramo, ta.tipo_tramo, tra.recorrido_adyacente
from tramo_almeria ta
join tramo_recorrido_almeria tra on ta.id_tramo = tra.id_tramo
where ta.tipo_tramo = 1 and tra.recorrido_adyacente = true


SELECT f.id_fotograma, f.id_video id_video_fotograma, v.id_video id_video_video
FROM fotograma_almeria f
LEFT JOIN video_almeria v
ON f.id_video = v.id_video
WHERE NOT ST_Intersects(f.geom, v.geom);


SELECT f.id_fotograma, f.id_video id_video_fotograma, v.id_video id_video_video
FROM fotograma_almeria f
LEFT JOIN video_almeria v
ON f.id_video <> v.id_video
WHERE ST_Intersects(f.geom, v.geom);

-- que los id_tramo de la tabla tramo_recorrido están en la tabla tramo del lote de ejes.
select tr.id_tramo id_tramo_desde_tramo_recorrido, ta.id_tramo id_tramo_desde_tramo
from tramo_recorrido_almeria tr
right outer join tramo_almeria ta on tr.id_tramo = ta.id_tramo
where ta.id_tramo is null
order by id_tramo_desde_tramo_recorrido;
-- esta igual que la anterior pero mejor
SELECT tr.id_tramo id_tramo_desde_tramo_recorrido, ta.id_tramo id_tramo_desde_tramo
FROM tramo_recorrido_almeria tr
FULL OUTER JOIN tramo_almeria ta ON tr.id_tramo = ta.id_tramo
WHERE tr.id_tramo IS NULL OR ta.id_tramo IS NULL
ORDER BY COALESCE(tr.id_tramo, ta.id_tramo);



