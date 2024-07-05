import psycopg2
import csv
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
#nom_provincia = input("Por favor, introduzca el nombre de la provincia todo en mayúsculas y sin tildes: ")
nom_provincia = 'almeria'.upper()

# Lista de consultas para csv
consultas_csv = [
    (
        "rutas",
        f"""
SELECT " 
    "REPLACE(CONCAT('\\{nom_provincia}\\', r.agrupacion, '\\', "
            "CASE WHEN r.tipo_recorrido = 1 THEN c.codigo || '\\' || c.codigo || '_' || r.sentido || '_' "
                "WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'"
                "WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'"
            "ELSE '' END,"
            "TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', " 
            "LPAD(r.num_secuencia::text, 2, '0'), '\\P\\', v.nombre, '.m4v'"
        ") || ';',"
        "';',"
        "E'\n'"
    ") AS resultado_total "
"FROM recorrido_{nom_provincia} r "
"JOIN carretera_{nom_provincia} c ON r.id_carretera = c.id_carretera "
"JOIN video_{nom_provincia} v ON r.id_recorrido = v.id_recorrido "
"UNION ALL "
"SELECT "
    "REPLACE(CONCAT('\\{nom_provincia}\\', r.agrupacion, '\\', "
            "CASE WHEN r.tipo_recorrido = 1 THEN c.codigo || '\\' || c.codigo || '_' || r.sentido || '_' "
                "WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'"
                "WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'"
            "ELSE '' END,"
            "TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', " 
            "LPAD(r.num_secuencia::text, 2, '0'), '\\M\\', v.nombre, '.m4v'"
        ") || ';',"
        "';',"
        "E'\n'"
    ") AS resultado_total "
"FROM recorrido_{nom_provincia} r "
"JOIN carretera_{nom_provincia} c ON r.id_carretera = c.id_carretera "
"JOIN video_{nom_provincia} v ON r.id_recorrido = v.id_recorrido "
"UNION ALL "
"SELECT "
    "REPLACE(CONCAT('\\{nom_provincia}\\', r.agrupacion, '\\', "
            "CASE WHEN r.tipo_recorrido = 1 THEN c.codigo || '\\' || c.codigo || '_' || r.sentido || '_' "
                "WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'"
                "WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'"
            "ELSE '' END,"
            "TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', " 
            "LPAD(r.num_secuencia::text, 2, '0'), '\\G\\', v.nombre, '.m4v'"
        ") || ';',"
        "';',"
        "E'\n'"
    ") AS resultado_total "
"FROM recorrido_{nom_provincia} r "
"JOIN carretera_{nom_provincia} c ON r.id_carretera = c.id_carretera "
"JOIN video_{nom_provincia} v ON r.id_recorrido = v.id_recorrido "
"UNION ALL "
"SELECT "
    "REPLACE(CONCAT('\\{nom_provincia}\\', r.agrupacion, '\\', "
            "CASE WHEN r.tipo_recorrido = 1 THEN c.codigo || '\\' || c.codigo || '_' || r.sentido || '_' "
                "WHEN r.tipo_recorrido = 2 THEN c.codigo || '\E_' || r.enlace || '_'"
                "WHEN r.tipo_recorrido = 3 THEN c.codigo || '\V_' || r.enlace || '_'"
            "ELSE '' END,"
            "TO_CHAR(r.fecha_grabacion, 'YYYYMMDD'), '_', " 
            "LPAD(r.num_secuencia::text, 2, '0'), '\\ORIGINAL\\', v.nombre, '.m4v'"
        ") || ';',"
        "';',"
        "E'\n'"
    ") AS resultado_total "
"FROM recorrido_{nom_provincia} r "
"JOIN carretera_{nom_provincia} c ON r.id_carretera = c.id_carretera "
"JOIN video_{nom_provincia} v ON r.id_recorrido = v.id_recorrido;
"""
),    
]

salida_csv = f"C:/Users/guillermocastro/Desktop/Guillermo/MITMA/QC_FME_NUEVO/{nom_provincia}/gpkg_{nom_provincia}.csv"

# Crear la carpeta si no existe
carpeta_salida = os.path.dirname(salida_csv)
if not os.path.exists(carpeta_salida):
    os.makedirs(carpeta_salida)

# Abrir el archivo CSV en modo escritura
with open(salida_csv, "w", newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    
    # Escribir encabezado
    csv_writer.writerow([f"RUTAS_{nom_provincia}"])
    
    # Iterar sobre las explicaciones y consultas
    for explicacion, consulta in consultas_csv:        
        # Ejecutar la consulta
        cur.execute(consulta)
        
        # Obtener resultados
        resultados = cur.fetchall()
        
        # Escribir la explicación y los resultados en el archivo CSV
        for row in resultados:
            csv_writer.writerow([row[0]])
            
# Cerrar el cursor y la conexión
cur.close()
conn.close()