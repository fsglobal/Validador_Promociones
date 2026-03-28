import os
import pandas as pd
from datetime import datetime

from db import obtener_conexion
from gestor_utils import leer_hoja_completar_gestor, detectar_columnas_repositorio


def obtener_excel_repositorio():
    base_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(base_dir)
    carpeta = os.path.join(root_dir, "ExcelRepositorio")

    if not os.path.exists(carpeta):
        print("No existe carpeta ExcelRepositorio")
        return []

    return sorted([
        os.path.join(carpeta, f)
        for f in os.listdir(carpeta)
        if f.lower().endswith(".xlsx")
    ])


def limpiar_tabla_promociones():
    conn = obtener_conexion()
    cur = conn.cursor()
    cur.execute("DELETE FROM promociones")
    conn.commit()
    conn.close()
    print("Tabla promociones vaciada correctamente.")


def extraer_registros_desde_excel(ruta_excel):
    print(f"\nProcesando: {os.path.basename(ruta_excel)}")

    df = leer_hoja_completar_gestor(ruta_excel)

    if df.empty:
        print("Sin datos en hoja COMPLETAR")
        return []

    cols = detectar_columnas_repositorio(df)

    col_id = cols["id_promocion"]
    col_sku = cols["sku"]
    col_desc = cols["descripcion"]
    col_precio = cols["precio"]
    col_pct = cols["porcentaje"]
    col_ini = cols["fecha_inicio"]
    col_fin = cols["fecha_fin"]
    col_mec = cols["mecanica"]

    if not col_id and not col_sku:
        print("No se detectaron columnas clave")
        return []

    registros = []

    for _, row in df.iterrows():
        id_promocion = str(row.get(col_id, "")).strip() if col_id else ""
        sku = str(row.get(col_sku, "")).strip() if col_sku else ""
        descripcion = str(row.get(col_desc, "")).strip() if col_desc else ""
        precio = str(row.get(col_precio, "")).strip() if col_precio else ""
        porcentaje = str(row.get(col_pct, "")).strip() if col_pct else ""
        fecha_inicio = str(row.get(col_ini, "")).strip() if col_ini else ""
        fecha_fin = str(row.get(col_fin, "")).strip() if col_fin else ""
        mecanica = str(row.get(col_mec, "")).strip() if col_mec else ""

        if not any([id_promocion, sku, fecha_inicio, fecha_fin, precio, porcentaje]):
            continue

        registros.append({
            "archivo_origen": os.path.basename(ruta_excel),
            "hoja_origen": "COMPLETAR",
            "id_promocion": id_promocion,
            "sku": sku,
            "descripcion": descripcion,
            "precio": precio,
            "porcentaje": porcentaje,
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "mecanica": mecanica,
            "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    print(f"Filas detectadas: {len(registros)}")
    return registros


def insertar_registros(registros):
    conn = obtener_conexion()
    cur = conn.cursor()

    total = 0

    for r in registros:
        cur.execute(
            """
            INSERT INTO promociones (
                archivo_origen,
                hoja_origen,
                id_promocion,
                sku,
                descripcion,
                precio,
                porcentaje,
                fecha_inicio,
                fecha_fin,
                mecanica,
                fecha_carga
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["archivo_origen"],
                r["hoja_origen"],
                r["id_promocion"],
                r["sku"],
                r["descripcion"],
                r["precio"],
                r["porcentaje"],
                r["fecha_inicio"],
                r["fecha_fin"],
                r["mecanica"],
                r["fecha_carga"],
            )
        )
        total += 1

    conn.commit()
    conn.close()

    print(f"Registros insertados: {total}")


def main():
    excels = obtener_excel_repositorio()
    print(f"Excel detectados: {len(excels)}")

    if not excels:
        return

    limpiar_tabla_promociones()

    total_registros = []

    for excel in excels:
        try:
            datos = extraer_registros_desde_excel(excel)
            total_registros.extend(datos)
        except Exception as e:
            print(f"Error procesando {os.path.basename(excel)}: {e}")

    print(f"\nTotal registros a insertar: {len(total_registros)}")

    if total_registros:
        insertar_registros(total_registros)
    else:
        print("No hubo registros para insertar.")


if __name__ == "__main__":
    main()