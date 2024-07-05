import psycopg2
import os

# Conectar con la base de datos
conn = psycopg2.connect("dbname='MITMA' user='postgres' password='pgadmin' host='localhost' port='5432'")

# Verificar si la conexión se realizó correctamente
if conn:
    print("Conexión exitosa a la base de datos.")
else:
    print("No se pudo establecer la conexión a la base de datos.")

# Se crea un cursor
cur = conn.cursor()

# Nombre de la provincia
#nom_provincia = input("Por favor, introduzca el nombre de la provincia en minúsculas y sin tildes: ")
nom_provincia = 'murcia'

# Lista de consultas y explicaciones para txt
consultas_txt = [
    (
        f"* Que no haya nulos en 'tramo_recorrido_{nom_provincia}'. False = No hay nulos, True = Hay nulos y habría que revisarlos.",
        f"SELECT EXISTS (SELECT 1 FROM tramo_recorrido_{nom_provincia} WHERE id IS NULL " 
            "OR id_tramo is NULL "
            "OR id_recorrido IS NULL "
            "OR recorrido_adyacente IS NULL "
        f") AS existen_nulls_en_tramo_recorrido_{nom_provincia}"
    ),
    (
        f"* Que no haya nulos en 'video_{nom_provincia}'. False = No hay nulos, True = Hay nulos y habría que revisarlos.",
        f"SELECT EXISTS (SELECT 1 FROM video_{nom_provincia} WHERE id_video IS NULL " 
            "OR id_recorrido is NULL "
            "OR id_camara IS NULL "
            "OR lote IS NULL "
            "OR nombre IS NULL "
            "OR fecha_grabacion IS NULL "
        f") AS existen_nulls_en_video_{nom_provincia}"
    ),
    (
        f"* Que no haya nulos en 'recorrido_{nom_provincia}'. Si sale vacío es que está correcto. \n\n"
        "id_carretera | agrupacion | sentido | enlace | tipo_recorrido | num_secuencia | fecha_grabacion",
        f"SELECT * FROM recorrido_{nom_provincia} WHERE id_recorrido is NULL "
            "OR id_carretera is NULL "
            "OR agrupacion IS NULL "
            "OR sentido IS NULL "
            "OR tipo_recorrido IS NULL "
            "OR num_secuencia IS NULL "
            "OR fecha_grabacion IS NULL "
            "OR ((tipo_recorrido = 2 OR tipo_recorrido = 3) AND (enlace IS NULL OR enlace = '')) "
            "OR (tipo_recorrido = 1 AND (enlace IS NOT NULL AND enlace != ''))"
    ),
    (
        f"* Que no haya nulos en 'fotograma_{nom_provincia}'. False = No hay nulos, True = Hay nulos y habría que revisarlos.",
        f"SELECT EXISTS (SELECT 1 FROM fotograma_{nom_provincia} WHERE id_fotograma IS NULL " 
            "OR id_video is NULL "
            "OR fotograma IS NULL "
            "OR tiempo IS NULL "
            "OR orientacion IS NULL "
        f") AS existen_nulls_en_fotograma_{nom_provincia}"
    ),
    (
        f"* Que todos los tramos de la tabla 'tramo_{nom_provincia}' aparezcan en la tabla 'tramo_recorrido_{nom_provincia}'. Si aparece vacío está correcto. \n\n"
        f"id_tramo tramo_{nom_provincia} | mensaje",
        f"select ta.id_tramo, 'Este no aparece en la tabla tramo_recorrido' AS mensaje from tramo_{nom_provincia} ta "
        f"where ta.id_tramo not in(select tra.id_tramo from tramo_recorrido_{nom_provincia} tra) "
        "order by ta.id_tramo"
    ),
    (
        "* Los tramos troncales no pueden tener recorrido_adyacente verdadero. \n\n"
        "id_tramo | tipo_tramo | recorrido_adyacente",
        "select ta.id_tramo, ta.tipo_tramo, tra.recorrido_adyacente "
        f"from tramo_{nom_provincia} ta "
        f"join tramo_recorrido_{nom_provincia} tra on ta.id_tramo = tra.id_tramo "
        "where ta.tipo_tramo = 1 and tra.recorrido_adyacente = true"
    ),
    (
        f"* Que todos los recorridos de la tabla 'recorrido_{nom_provincia}' aparecen en la tabla 'tramo_recorrido_{nom_provincia}'. Si aparece vacío está correcto.\n\n"
        f"id_recorrido recorrido_{nom_provincia} | id_recorrido tramo_recorrido_{nom_provincia} | mensaje \n",
        f"SELECT ra.id_recorrido AS id_recorrido_de_la_tabla_recorrido_{nom_provincia}, NULL AS id_recorrido_de_la_tabla_tramo_recorrido_{nom_provincia}, "
            "CASE WHEN ra.id_recorrido IS NOT NULL AND tra.id_recorrido IS NULL THEN 'sobran vídeos o error de asignación' " 
                "ELSE '' END AS mensaje "
        f"FROM recorrido_{nom_provincia} ra "
        f"LEFT JOIN (SELECT tra.id_recorrido FROM tramo_recorrido_{nom_provincia} tra) tra "
        "ON ra.id_recorrido = tra.id_recorrido "
        "WHERE tra.id_recorrido IS NULL "
        "UNION "
        f"SELECT NULL AS id_recorrido_{nom_provincia}, tra.id_recorrido AS id_recorrido_tramo_{nom_provincia}, "
            "CASE WHEN tra.id_recorrido IS NOT NULL AND ra.id_recorrido IS NULL THEN 'falta vídeos o error de asignación' " 
                "ELSE '' END AS mensaje "
        f"FROM tramo_recorrido_{nom_provincia} tra "
        f"LEFT JOIN (SELECT ra.id_recorrido FROM recorrido_{nom_provincia} ra) ra "
        "ON tra.id_recorrido = ra.id_recorrido "
        "WHERE ra.id_recorrido IS NULL;"
    ),
    (
        f"* Que todos los recorridos de la tabla recorrido_{nom_provincia} aparezcan en la tabla video_{nom_provincia}. Si aparece vacío está correcto. \n\n"
        f"id_recorrido recorrido_{nom_provincia} | mensaje",
        "SELECT ra.id_recorrido, 'Este no aparece en la tabla video' AS mensaje "
        f"FROM recorrido_{nom_provincia} ra "
        f"WHERE ra.id_recorrido NOT IN (SELECT id_recorrido FROM video_{nom_provincia}) "
        "ORDER BY ra.id_recorrido;"
    ),
    (
        "* Que todos los videos tengan fotogramas. Si aparece vacío está correcto. \n\n"
        f"id_video video_{nom_provincia} | mensaje",
        "select va.id_video, 'Este video no tiene fotograma' AS mensaje "
        f"from video_{nom_provincia} va "
        f"where va.id_video not in(select fa.id_video f from fotograma_{nom_provincia} fa) "
        "order by va.id_video;"
    ),
    (
        "* Que todos los recorridos con tipo_recorrido = 1 solo pueden tener sentido 1 o 2. Y los tipo_recorrido = 2 o 3 solo pueden tener sentido -998. \n"
        "Solo se verán los incorrectos \n\n"
        "sentido | tipo_recorrido | validación",
        "select * from (select ra.sentido, ra.tipo_recorrido, "
            "case when ra.tipo_recorrido='1' then " 
                    "case when ra.sentido = '1' or ra.sentido= '2' then 'Correcto' "
                        "else 'Incorrecto' end "
                "when ra.tipo_recorrido='2' or ra.tipo_recorrido='3' then " 
                    "case when ra.sentido = '-998'then 'Correcto' "
                        "else 'Incorrecto' end "
                "else 'Incorrecto' "
            "end as validacion "	
        f"from recorrido_{nom_provincia} ra) as sub "
        "where validacion = 'Incorrecto'"
    ),
    (
        "* Resultado de aquellos, número de secuencia y número de secuencia esperada, que no son iguales. Si aparece vacío está correcto.\n\n"
        "   id_recorrido | num_secuencia | num_secuencia_esperada",
        ("select id_recorrido, num_secuencia, num_secuencia_esperada from(SELECT id_recorrido, num_secuencia,"
			   "ROW_NUMBER() OVER (PARTITION BY id_carretera, agrupacion, sentido, enlace, tipo_recorrido " 
								  "ORDER BY id_carretera, tipo_recorrido, enlace, sentido, num_secuencia) AS num_secuencia_esperada "
            f"FROM recorrido_{nom_provincia} "
            "ORDER BY id_carretera, tipo_recorrido, enlace, sentido, num_secuencia) as subconsulta "
        "where num_secuencia <> num_secuencia_esperada")
    ),
    (
        f"* El sentido de la tabla recorrido_{nom_provincia} tiene que ser el mismo que sentido tabla tramo_carretera_{nom_provincia}. Si aparece vacío está correcto.\n\n"
        "id_tramo |   sentido    |      sentido \n"
        "\t | (recorrido)  | (tramo_carretera)",
        ("select tra.id_tramo, ra.sentido sentido_recorrido, tca.sentido sentido_tramo_carretera "		
        f"from recorrido_{nom_provincia} ra "
        f"join tramo_recorrido_{nom_provincia} tra on tra.id_recorrido = ra.id_recorrido "
        f"join tramo_carretera_{nom_provincia} tca on tca.id_tramo = tra.id_tramo "
        "where (ra.sentido = '1' or ra.sentido='2') and (tca.sentido = '1' or tca.sentido='2') and ra.sentido <> tca.sentido and tra.recorrido_adyacente=false "
        "order by tra.id_tramo;")
    ),
    (
        f"* El campo tipo_tramo de la capa de {nom_provincia} tiene que ser igual que el campo tipo_recorrido de la tabla recorrido de la capa de {nom_provincia}. \n"
        "Entonces, esta consulta muestra aquellos que no son iguales, por lo que, habría que panear el resultado. \n" 
        "LA MAYORÍA DE LOS CASOS SERÁN ROTONDAS Y, POR LO TANTO, FALSO POSITIVOS. \n\n"
        " id_tramo | tipo_tramo | tipo_recorrido | recorrido_adyacente",
        "SELECT tra.id_tramo, sub.tipo_tramo_transformado AS tipo_tramo, ra.tipo_recorrido, tra.recorrido_adyacente "
        f"FROM tramo_recorrido_{nom_provincia} tra "
        "JOIN (SELECT ta.id_tramo, ta.tipo_tramo, "
                "CASE WHEN ta.tipo_tramo IN (4, 21, 22, 23, 24, 25, 26, 51, 52, 53, 91, 99) THEN 2 "
                    "WHEN ta.tipo_tramo IN (31, 32, 33) THEN 3 "
                    "ELSE ta.tipo_tramo "
                "END AS tipo_tramo_transformado "
            f"FROM tramo_{nom_provincia} ta) AS sub ON tra.id_tramo = sub.id_tramo "
        f"JOIN recorrido_{nom_provincia} ra ON tra.id_recorrido = ra.id_recorrido "
        "WHERE sub.tipo_tramo_transformado <> ra.tipo_recorrido and tra.recorrido_adyacente=false "
        "ORDER BY tra.id_tramo;"
    ),
    (
        "* Se comprueban que no haya fotogramas duplicados entre 'fotograma' y 'next_fotograma', así como la distancia entre ellos. \n"
        "Cuando 'fotograma' es igual que 'next_fotograma' es un error. Habría que revisar aquellas distancias menores a 2 m aunque seguramente sean falsos positivos. \n"
        "Aquellas que sean mayores a 12 m son errores. \n\n"
        "    id_video    |   fotograma   | next_fotograma |   distance ",
        "WITH ordered_fotogramas AS (SELECT id_video, fotograma, geom, "
                "LEAD(geom) OVER (PARTITION BY id_video ORDER BY fotograma) AS next_geom, "
                "LEAD(fotograma) OVER (PARTITION BY id_video ORDER BY fotograma) AS next_fotograma "
            f"FROM fotograma_{nom_provincia}), "
        "distances AS (SELECT id_video, fotograma, next_fotograma, "
                "round(ST_Distance(ST_Transform(geom, 32633), ST_Transform(next_geom, 32633))::numeric,2) AS distance "
            "FROM ordered_fotogramas "
            "WHERE next_geom IS NOT NULL) "
        "SELECT id_video, fotograma, next_fotograma, distance "
        "FROM distances "
        "WHERE distance < 2 or distance > 12 "
        "ORDER BY distance desc;"
    ),
    (
        f"* Se comprueban que los id_tramo de la tabla tramo_recorrido_{nom_provincia} estén en la tabla tramo del lote de ejes \n\n"
        "id_tramo_desde_tramo_recorrido | id_tramo_desde_tramo",
        "SELECT tr.id_tramo id_tramo_desde_tramo_recorrido, ta.id_tramo id_tramo_desde_tramo "
        f"FROM tramo_recorrido_{nom_provincia} tr "
        f"FULL OUTER JOIN tramo_{nom_provincia} ta ON tr.id_tramo = ta.id_tramo "
        "WHERE tr.id_tramo IS NULL OR ta.id_tramo IS NULL "
        "ORDER BY COALESCE(tr.id_tramo, ta.id_tramo);"
    ),
    (
        f"* Rutas de la tabla 'gpkg_{nom_provincia}' que no están presentes en la tabla 'lista_rutas_video_{nom_provincia}' de Alberto.",
        f"SELECT rutas FROM gpkg_{nom_provincia} WHERE rutas NOT IN (SELECT rutas FROM lista_rutas_video_{nom_provincia})"
    )
]

# Nombre archivo salida
salida_txt = f"C:/Users/guillermocastro/Desktop/Guillermo/MITMA/QC_FME_NUEVO/{nom_provincia}/control_calidad_{nom_provincia}.txt"

# Crear la carpeta si no existe
carpeta_salida = os.path.dirname(salida_txt)
if not os.path.exists(carpeta_salida):
    os.makedirs(carpeta_salida)

# Guardar los resultados y las explicaciones en un archivo de texto
with open(salida_txt, "w") as f:
    f.write("******PARA EL CASO DE "+nom_provincia.upper()+"******\n\n\n")
    for explicacion, consulta in consultas_txt:        
        # Escribir la explicación
        f.write(explicacion + "\n\n")
        
        # Ejecutar la consulta
        cur.execute(consulta)
        
        # Obtener resultados
        resultados = cur.fetchall()
        
        # Escribir los resultados de la consulta
        for i,row in enumerate(resultados, start=1):
            f.write(f'{i}. ' + "\t|\t".join(map(str, row)) + "\n")
        
        f.write("\n\n")

# Cerrar el cursor y la conexión
cur.close()
conn.close()