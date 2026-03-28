import os
import pandas as pd
from flask import render_template, request

from db import obtener_conexion
from gestor_utils import procesar_excels_repositorio
from gestor_utils_eventos import procesar_excel_eventos


# ============================================================
# HELPERS GENERALES
# ============================================================

def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def es_nombre_hoja_mes(nombre):
    nombre = normalizar_texto(nombre).lower()
    meses = {
        "enero", "ene",
        "febrero", "feb",
        "marzo", "mar",
        "abril", "abr",
        "mayo", "may",
        "junio", "jun",
        "julio", "jul",
        "agosto", "ago",
        "septiembre", "sept", "setiembre", "sep", "set",
        "octubre", "oct",
        "noviembre", "nov",
        "diciembre", "dic",
    }
    return nombre in meses


def es_excel_eventos(ruta_excel):
    try:
        xls = pd.ExcelFile(ruta_excel)
        hojas = [normalizar_texto(h) for h in xls.sheet_names]

        tiene_codigo_marca = any(
            h.lower() == "codigo-marca" or h.lower() == "codigo marca"
            for h in hojas
        )
        tiene_hoja_mes = any(es_nombre_hoja_mes(h) for h in hojas)

        return tiene_codigo_marca and tiene_hoja_mes

    except Exception as e:
        print(f"✗ Error detectando tipo de archivo {os.path.basename(ruta_excel)}: {e}")
        return False


def es_excel_promociones(ruta_excel):
    try:
        xls = pd.ExcelFile(ruta_excel)
        hojas = [normalizar_texto(h).upper() for h in xls.sheet_names]
        return "COMPLETAR" in hojas
    except Exception as e:
        print(f"✗ Error detectando archivo de promociones {os.path.basename(ruta_excel)}: {e}")
        return False


# ============================================================
# HELPERS SQLITE - CONTADORES
# ============================================================

def contar_registros_promociones():
    conn = obtener_conexion()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) AS total FROM promociones").fetchone()["total"]
    conn.close()
    return total or 0


def contar_registros_eventos():
    conn = obtener_conexion()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) AS total FROM eventos").fetchone()["total"]
    conn.close()
    return total or 0


def contar_registros_evento_skus():
    conn = obtener_conexion()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) AS total FROM evento_skus").fetchone()["total"]
    conn.close()
    return total or 0


# ============================================================
# HELPERS SQLITE - PROMOCIONES
# ============================================================

def existe_registro_promocion(cur, registro):
    sql = """
        SELECT 1
        FROM promociones
        WHERE TRIM(COALESCE(archivo_origen, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(id_promocion, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(sku, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_inicio, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_fin, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(precio, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(porcentaje, '')) = TRIM(COALESCE(?, ''))
        LIMIT 1
    """

    fila = cur.execute(
        sql,
        (
            normalizar_texto(registro.get("archivo_origen", "")),
            normalizar_texto(registro.get("id_promocion", "")),
            normalizar_texto(registro.get("sku", "")),
            normalizar_texto(registro.get("fecha_inicio", "")),
            normalizar_texto(registro.get("fecha_fin", "")),
            normalizar_texto(registro.get("precio", "")),
            normalizar_texto(registro.get("porcentaje", "")),
        ),
    ).fetchone()

    return fila is not None


def insertar_dataframe_promociones(df):
    if df.empty:
        return 0, 0

    conn = obtener_conexion()
    cur = conn.cursor()

    insertados = 0
    duplicados = 0

    for _, row in df.iterrows():
        registro = {
            "fecha_carga": normalizar_texto(row.get("fecha_carga", "")),
            "archivo_origen": normalizar_texto(row.get("archivo_origen", "")),
            "hoja_origen": normalizar_texto(row.get("hoja_origen", "")),
            "id_promocion": normalizar_texto(row.get("id_promocion", "")),
            "sku": normalizar_texto(row.get("sku", "")),
            "descripcion": normalizar_texto(row.get("descripcion", "")),
            "precio": normalizar_texto(row.get("precio", "")),
            "porcentaje": normalizar_texto(row.get("porcentaje", "")),
            "fecha_inicio": normalizar_texto(row.get("fecha_inicio", "")),
            "fecha_fin": normalizar_texto(row.get("fecha_fin", "")),
            "mecanica": normalizar_texto(row.get("mecanica", "")),
        }

        if existe_registro_promocion(cur, registro):
            duplicados += 1
            continue

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
                registro["archivo_origen"],
                registro["hoja_origen"],
                registro["id_promocion"],
                registro["sku"],
                registro["descripcion"],
                registro["precio"],
                registro["porcentaje"],
                registro["fecha_inicio"],
                registro["fecha_fin"],
                registro["mecanica"],
                registro["fecha_carga"],
            ),
        )
        insertados += 1

    conn.commit()
    conn.close()

    return insertados, duplicados


# ============================================================
# HELPERS SQLITE - EVENTOS
# ============================================================

def existe_registro_evento(cur, registro):
    sql = """
        SELECT 1
        FROM eventos
        WHERE TRIM(COALESCE(archivo_origen, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(hoja_origen, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(id_evento, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(marca, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(local, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_inicio, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_fin, '')) = TRIM(COALESCE(?, ''))
        LIMIT 1
    """

    fila = cur.execute(
        sql,
        (
            normalizar_texto(registro.get("archivo_origen", "")),
            normalizar_texto(registro.get("hoja_origen", "")),
            normalizar_texto(registro.get("id_evento", "")),
            normalizar_texto(registro.get("marca", "")),
            normalizar_texto(registro.get("local", "")),
            normalizar_texto(registro.get("fecha_inicio", "")),
            normalizar_texto(registro.get("fecha_fin", "")),
        ),
    ).fetchone()

    return fila is not None


def existe_registro_evento_sku(cur, registro):
    sql = """
        SELECT 1
        FROM evento_skus
        WHERE TRIM(COALESCE(archivo_origen, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(hoja_origen, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(id_evento, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(marca, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(sku, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(local, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_inicio, '')) = TRIM(COALESCE(?, ''))
          AND TRIM(COALESCE(fecha_fin, '')) = TRIM(COALESCE(?, ''))
        LIMIT 1
    """

    fila = cur.execute(
        sql,
        (
            normalizar_texto(registro.get("archivo_origen", "")),
            normalizar_texto(registro.get("hoja_origen", "")),
            normalizar_texto(registro.get("id_evento", "")),
            normalizar_texto(registro.get("marca", "")),
            normalizar_texto(registro.get("sku", "")),
            normalizar_texto(registro.get("local", "")),
            normalizar_texto(registro.get("fecha_inicio", "")),
            normalizar_texto(registro.get("fecha_fin", "")),
        ),
    ).fetchone()

    return fila is not None


def insertar_dataframe_eventos(df_eventos):
    if df_eventos.empty:
        return 0, 0

    conn = obtener_conexion()
    cur = conn.cursor()

    insertados = 0
    duplicados = 0

    for _, row in df_eventos.iterrows():
        registro = {
            "archivo_origen": normalizar_texto(row.get("archivo_origen", "")),
            "hoja_origen": normalizar_texto(row.get("hoja_origen", "")),
            "id_evento": normalizar_texto(row.get("id_evento", "")),
            "rc": normalizar_texto(row.get("rc", "")),
            "numero_cam": normalizar_texto(row.get("numero_cam", "")),
            "local": normalizar_texto(row.get("local", "")),
            "lista_locales": normalizar_texto(row.get("lista_locales", "")),
            "lista_productos": normalizar_texto(row.get("lista_productos", "")),
            "fecha_inicio": normalizar_texto(row.get("fecha_inicio", "")),
            "fecha_fin": normalizar_texto(row.get("fecha_fin", "")),
            "marca": normalizar_texto(row.get("marca", "")),
            "descuento": normalizar_texto(row.get("descuento", "")),
            "personal_apoyo": normalizar_texto(row.get("personal_apoyo", "")),
            "rut_personal": normalizar_texto(row.get("rut_personal", "")),
            "tipo_evento": normalizar_texto(row.get("tipo_evento", "")),
            "evento_cerrado_con_dermo": normalizar_texto(row.get("evento_cerrado_con_dermo", "")),
            "financiamiento": normalizar_texto(row.get("financiamiento", "")),
            "facturar_a": normalizar_texto(row.get("facturar_a", "")),
            "tipo_pago": normalizar_texto(row.get("tipo_pago", "")),
            "fecha_carga": normalizar_texto(row.get("fecha_carga", "")),
        }

        if existe_registro_evento(cur, registro):
            duplicados += 1
            continue

        cur.execute(
            """
            INSERT INTO eventos (
                archivo_origen,
                hoja_origen,
                id_evento,
                rc,
                numero_cam,
                local,
                lista_locales,
                lista_productos,
                fecha_inicio,
                fecha_fin,
                marca,
                descuento,
                personal_apoyo,
                rut_personal,
                tipo_evento,
                evento_cerrado_con_dermo,
                financiamiento,
                facturar_a,
                tipo_pago,
                fecha_carga
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                registro["archivo_origen"],
                registro["hoja_origen"],
                registro["id_evento"],
                registro["rc"],
                registro["numero_cam"],
                registro["local"],
                registro["lista_locales"],
                registro["lista_productos"],
                registro["fecha_inicio"],
                registro["fecha_fin"],
                registro["marca"],
                registro["descuento"],
                registro["personal_apoyo"],
                registro["rut_personal"],
                registro["tipo_evento"],
                registro["evento_cerrado_con_dermo"],
                registro["financiamiento"],
                registro["facturar_a"],
                registro["tipo_pago"],
                registro["fecha_carga"],
            ),
        )
        insertados += 1

    conn.commit()
    conn.close()

    return insertados, duplicados


def insertar_dataframe_evento_skus(df_evento_skus):
    if df_evento_skus.empty:
        return 0, 0

    conn = obtener_conexion()
    cur = conn.cursor()

    insertados = 0
    duplicados = 0

    for _, row in df_evento_skus.iterrows():
        registro = {
            "id_evento": normalizar_texto(row.get("id_evento", "")),
            "marca": normalizar_texto(row.get("marca", "")),
            "sku": normalizar_texto(row.get("sku", "")),
            "descripcion": normalizar_texto(row.get("descripcion", "")),
            "archivo_origen": normalizar_texto(row.get("archivo_origen", "")),
            "hoja_origen": normalizar_texto(row.get("hoja_origen", "")),
            "fecha_inicio": normalizar_texto(row.get("fecha_inicio", "")),
            "fecha_fin": normalizar_texto(row.get("fecha_fin", "")),
            "local": normalizar_texto(row.get("local", "")),
            "fecha_carga": normalizar_texto(row.get("fecha_carga", "")),
        }

        if existe_registro_evento_sku(cur, registro):
            duplicados += 1
            continue

        cur.execute(
            """
            INSERT INTO evento_skus (
                id_evento,
                marca,
                sku,
                descripcion,
                archivo_origen,
                hoja_origen,
                fecha_inicio,
                fecha_fin,
                local,
                fecha_carga
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                registro["id_evento"],
                registro["marca"],
                registro["sku"],
                registro["descripcion"],
                registro["archivo_origen"],
                registro["hoja_origen"],
                registro["fecha_inicio"],
                registro["fecha_fin"],
                registro["local"],
                registro["fecha_carga"],
            ),
        )
        insertados += 1

    conn.commit()
    conn.close()

    return insertados, duplicados


# ============================================================
# PROCESAMIENTO POR ARCHIVO
# ============================================================

def procesar_archivo_promociones(ruta_excel):
    carpeta_temporal = os.path.dirname(ruta_excel)
    df_total, resumen = procesar_excels_repositorio(carpeta_temporal)

    nombre_archivo = os.path.basename(ruta_excel)
    if not df_total.empty and "archivo_origen" in df_total.columns:
        df_total = df_total[
            df_total["archivo_origen"].astype(str).str.strip() == nombre_archivo
        ].copy()

    if not df_total.empty:
        df_total = df_total.fillna("").astype(str)

        columnas_clave = [
            "archivo_origen",
            "id_promocion",
            "sku",
            "fecha_inicio",
            "fecha_fin",
            "precio",
            "porcentaje",
        ]
        columnas_presentes = [c for c in columnas_clave if c in df_total.columns]
        if columnas_presentes:
            antes = len(df_total)
            df_total = df_total.drop_duplicates(
                subset=columnas_presentes,
                keep="first"
            ).reset_index(drop=True)
            duplicados_lote = antes - len(df_total)
        else:
            duplicados_lote = 0
    else:
        duplicados_lote = 0

    insertados, duplicados_db = insertar_dataframe_promociones(df_total)

    return {
        "tipo": "PROMOCIONES",
        "archivo": nombre_archivo,
        "registros_detectados": len(df_total),
        "insertados": insertados,
        "duplicados_lote": duplicados_lote,
        "duplicados_db": duplicados_db,
        "resumen": resumen,
    }


def procesar_archivo_eventos(ruta_excel):
    rc_objetivo = "FERNANDO"

    df_eventos, df_evento_skus, resumen_hojas = procesar_excel_eventos(
        ruta_excel,
        rc_objetivo=rc_objetivo
    )

    if not df_eventos.empty:
        df_eventos = df_eventos.fillna("").astype(str)
        columnas_clave_eventos = [
            "archivo_origen",
            "hoja_origen",
            "id_evento",
            "marca",
            "local",
            "fecha_inicio",
            "fecha_fin",
        ]
        presentes = [c for c in columnas_clave_eventos if c in df_eventos.columns]
        if presentes:
            antes_eventos = len(df_eventos)
            df_eventos = df_eventos.drop_duplicates(
                subset=presentes,
                keep="first"
            ).reset_index(drop=True)
            duplicados_lote_eventos = antes_eventos - len(df_eventos)
        else:
            duplicados_lote_eventos = 0
    else:
        duplicados_lote_eventos = 0

    if not df_evento_skus.empty:
        df_evento_skus = df_evento_skus.fillna("").astype(str)
        columnas_clave_evento_skus = [
            "archivo_origen",
            "hoja_origen",
            "id_evento",
            "marca",
            "sku",
            "local",
            "fecha_inicio",
            "fecha_fin",
        ]
        presentes = [c for c in columnas_clave_evento_skus if c in df_evento_skus.columns]
        if presentes:
            antes_evento_skus = len(df_evento_skus)
            df_evento_skus = df_evento_skus.drop_duplicates(
                subset=presentes,
                keep="first"
            ).reset_index(drop=True)
            duplicados_lote_evento_skus = antes_evento_skus - len(df_evento_skus)
        else:
            duplicados_lote_evento_skus = 0
    else:
        duplicados_lote_evento_skus = 0

    insertados_eventos, duplicados_db_eventos = insertar_dataframe_eventos(df_eventos)
    insertados_evento_skus, duplicados_db_evento_skus = insertar_dataframe_evento_skus(df_evento_skus)

    return {
        "tipo": "EVENTOS",
        "archivo": os.path.basename(ruta_excel),
        "rc_objetivo": rc_objetivo,
        "eventos_detectados": len(df_eventos),
        "evento_skus_detectados": len(df_evento_skus),
        "insertados_eventos": insertados_eventos,
        "insertados_evento_skus": insertados_evento_skus,
        "duplicados_lote_eventos": duplicados_lote_eventos,
        "duplicados_lote_evento_skus": duplicados_lote_evento_skus,
        "duplicados_db_eventos": duplicados_db_eventos,
        "duplicados_db_evento_skus": duplicados_db_evento_skus,
        "resumen_hojas": resumen_hojas,
    }


# ============================================================
# RUTAS REPOSITORIO
# ============================================================

def registrar_rutas_repositorio(app):

    base_dir = os.path.dirname(__file__)

    if os.path.exists(os.path.join(base_dir, "ExcelRepositorio")):
        root_dir = base_dir
    else:
        root_dir = os.path.dirname(base_dir)

    carpeta_excel_repositorio = os.path.join(root_dir, "ExcelRepositorio")
    carpeta_data = os.path.join(root_dir, "data")

    os.makedirs(carpeta_data, exist_ok=True)
    os.makedirs(carpeta_excel_repositorio, exist_ok=True)

    def obtener_archivos_excel():
        if not os.path.exists(carpeta_excel_repositorio):
            return []

        return sorted([
            f for f in os.listdir(carpeta_excel_repositorio)
            if f.lower().endswith(".xlsx")
        ])

    # ============================================================
    # PANTALLA PRINCIPAL
    # ============================================================

    @app.route("/repositorio")
    def repositorio():
        archivos = obtener_archivos_excel()
        total_archivos = len(archivos)

        total_registros = (
            contar_registros_promociones()
            + contar_registros_eventos()
            + contar_registros_evento_skus()
        )

        return render_template(
            "repositorio.html",
            archivos=archivos,
            total_archivos=total_archivos,
            total_registros=total_registros,
            resumen=None,
            mensaje=""
        )

    # ============================================================
    # SUBIR EXCELS
    # ============================================================

    @app.route("/subir_excel_repositorio", methods=["POST"])
    def subir_excel_repositorio():
        archivos_subidos = 0

        for file in request.files.getlist("excel_files"):
            if file and file.filename and file.filename.lower().endswith(".xlsx"):
                ruta_destino = os.path.join(
                    carpeta_excel_repositorio,
                    file.filename
                )
                file.save(ruta_destino)
                archivos_subidos += 1

        archivos = obtener_archivos_excel()
        total_archivos = len(archivos)
        total_registros = (
            contar_registros_promociones()
            + contar_registros_eventos()
            + contar_registros_evento_skus()
        )

        mensaje = f"Se subieron {archivos_subidos} archivo(s) al repositorio."

        return render_template(
            "repositorio.html",
            archivos=archivos,
            total_archivos=total_archivos,
            total_registros=total_registros,
            resumen=None,
            mensaje=mensaje
        )

    # ============================================================
    # PROCESAR EXCELS -> SQLITE
    # ============================================================

    @app.route("/procesar_repositorio", methods=["POST"])
    def procesar_repositorio():
        print("\n" + "=" * 80)
        print("DEBUG REPOSITORIO SQLITE")

        archivos = obtener_archivos_excel()
        total_archivos = len(archivos)

        if not archivos:
            return render_template(
                "repositorio.html",
                archivos=[],
                total_archivos=0,
                total_registros=(
                    contar_registros_promociones()
                    + contar_registros_eventos()
                    + contar_registros_evento_skus()
                ),
                resumen=[],
                mensaje="No hay archivos Excel para procesar."
            )

        resumen_general = []

        total_promos_insertadas = 0
        total_promos_duplicados_lote = 0
        total_promos_duplicados_db = 0

        total_eventos_insertados = 0
        total_eventos_duplicados_lote = 0
        total_eventos_duplicados_db = 0

        total_evento_skus_insertados = 0
        total_evento_skus_duplicados_lote = 0
        total_evento_skus_duplicados_db = 0

        archivos_sin_tipo = []

        for archivo in archivos:
            ruta_excel = os.path.join(carpeta_excel_repositorio, archivo)

            print("-" * 80)
            print(f"Procesando archivo: {archivo}")

            try:
                if es_excel_eventos(ruta_excel):
                    print("Tipo detectado: EVENTOS")

                    resultado = procesar_archivo_eventos(ruta_excel)

                    total_eventos_insertados += resultado["insertados_eventos"]
                    total_eventos_duplicados_lote += resultado["duplicados_lote_eventos"]
                    total_eventos_duplicados_db += resultado["duplicados_db_eventos"]

                    total_evento_skus_insertados += resultado["insertados_evento_skus"]
                    total_evento_skus_duplicados_lote += resultado["duplicados_lote_evento_skus"]
                    total_evento_skus_duplicados_db += resultado["duplicados_db_evento_skus"]

                    resumen_general.append({
                        "archivo": archivo,
                        "estado": "OK",
                        "registros": resultado["eventos_detectados"],
                        "detalle": (
                            f"Tipo: EVENTOS | "
                            f"RC: {resultado['rc_objetivo']} | "
                            f"Eventos insertados: {resultado['insertados_eventos']} | "
                            f"Evento-SKU insertados: {resultado['insertados_evento_skus']}"
                        )
                    })

                elif es_excel_promociones(ruta_excel):
                    print("Tipo detectado: PROMOCIONES")

                    resultado = procesar_archivo_promociones(ruta_excel)

                    total_promos_insertadas += resultado["insertados"]
                    total_promos_duplicados_lote += resultado["duplicados_lote"]
                    total_promos_duplicados_db += resultado["duplicados_db"]

                    resumen_general.append({
                        "archivo": archivo,
                        "estado": "OK",
                        "registros": resultado["registros_detectados"],
                        "detalle": (
                            f"Tipo: PROMOCIONES | "
                            f"Insertados: {resultado['insertados']} | "
                            f"Duplicados BD: {resultado['duplicados_db']}"
                        )
                    })

                else:
                    print("Tipo detectado: DESCONOCIDO")
                    archivos_sin_tipo.append(archivo)

                    resumen_general.append({
                        "archivo": archivo,
                        "estado": "SIN PROCESAR",
                        "registros": 0,
                        "detalle": "No se pudo identificar si el archivo corresponde a promociones o eventos."
                    })

            except Exception as e:
                print(f"Error procesando {archivo}: {e}")
                resumen_general.append({
                    "archivo": archivo,
                    "estado": "ERROR",
                    "registros": 0,
                    "detalle": str(e)
                })

        partes_mensaje = []

        if (
            total_promos_insertadas > 0
            or total_promos_duplicados_lote > 0
            or total_promos_duplicados_db > 0
        ):
            partes_mensaje.append(
                "Promociones -> "
                f"insertadas: {total_promos_insertadas}, "
                f"duplicados lote: {total_promos_duplicados_lote}, "
                f"duplicados BD: {total_promos_duplicados_db}"
            )

        if (
            total_eventos_insertados > 0
            or total_eventos_duplicados_lote > 0
            or total_eventos_duplicados_db > 0
        ):
            partes_mensaje.append(
                "Eventos -> "
                f"insertados: {total_eventos_insertados}, "
                f"duplicados lote: {total_eventos_duplicados_lote}, "
                f"duplicados BD: {total_eventos_duplicados_db}"
            )

        if (
            total_evento_skus_insertados > 0
            or total_evento_skus_duplicados_lote > 0
            or total_evento_skus_duplicados_db > 0
        ):
            partes_mensaje.append(
                "Evento-SKU -> "
                f"insertados: {total_evento_skus_insertados}, "
                f"duplicados lote: {total_evento_skus_duplicados_lote}, "
                f"duplicados BD: {total_evento_skus_duplicados_db}"
            )

        if archivos_sin_tipo:
            partes_mensaje.append(
                "Archivos sin tipo reconocido: " + ", ".join(archivos_sin_tipo)
            )

        if not partes_mensaje:
            mensaje = (
                "Repositorio sin cambios. "
                "Todos los registros procesados ya existían en SQLite o no se detectaron datos válidos."
            )
        else:
            mensaje = " | ".join(partes_mensaje)

        print(mensaje)
        print("=" * 80 + "\n")

        total_registros = (
            contar_registros_promociones()
            + contar_registros_eventos()
            + contar_registros_evento_skus()
        )

        return render_template(
            "repositorio.html",
            archivos=archivos,
            total_archivos=total_archivos,
            total_registros=total_registros,
            resumen=resumen_general,
            mensaje=mensaje
        )
