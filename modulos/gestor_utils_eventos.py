import os
import re
import unicodedata
from datetime import datetime

import pandas as pd


# ============================================================
# UTILIDADES GENERALES
# ============================================================

def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def quitar_acentos(texto):
    texto = str(texto or "")
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def normalizar_columna(nombre):
    texto = str(nombre or "").strip().lower()
    texto = quitar_acentos(texto)
    texto = texto.replace("\n", " ")
    texto = texto.replace("\r", " ")
    texto = texto.replace("_", " ")
    texto = texto.replace("-", " ")
    texto = texto.replace(".", " ")
    texto = texto.replace("°", " ")
    texto = texto.replace("º", " ")
    texto = " ".join(texto.split())
    return texto


def normalizar_comparable(valor):
    return quitar_acentos(normalizar_texto(valor)).upper()


def valor_row(row, columna):
    if not columna:
        return ""
    return normalizar_texto(row.get(columna, ""))


def formatear_fecha_excel(valor):
    if valor is None or valor == "":
        return ""

    try:
        dt = pd.to_datetime(valor, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return normalizar_texto(valor)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return normalizar_texto(valor)


def es_nombre_hoja_mes(nombre):
    nombre_norm = normalizar_columna(nombre)

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

    return nombre_norm in meses


def buscar_hojas_mes(sheet_names):
    return [h for h in sheet_names if es_nombre_hoja_mes(h)]


# ============================================================
# DETECCIÓN DE HEADERS
# ============================================================

def detectar_fila_header_eventos(df_raw, max_filas=15):
    for i in range(min(max_filas, len(df_raw))):
        fila = " | ".join(df_raw.iloc[i].astype(str).str.upper().tolist())

        if (
            "MARCA" in fila
            or "LOCAL" in fila
            or "LISTA PRODUCTOS" in fila
            or "LISTA DE PRODUCTOS" in fila
            or "LISTA LOCAL" in fila
            or "LISTA DE LOCAL" in fila
            or "LISTA DE LOCALES" in fila
            or "TIPO DE EVENTO" in fila
            or "ID GEO" in fila
            or "FEHA DE INICIO EVENTO" in fila
            or "FECHA DE INICIO EVENTO" in fila
        ):
            return i

    return None


def detectar_fila_header_codigo_marca(df_raw, max_filas=15):
    for i in range(min(max_filas, len(df_raw))):
        fila = " | ".join(df_raw.iloc[i].astype(str).str.upper().tolist())

        if "CODIGO" in fila and "MARCA" in fila:
            return i

    return None


# ============================================================
# LECTURA DE HOJAS
# ============================================================

def leer_hoja_con_header_detectado(ruta_excel, sheet_name, detector_header):
    try:
        df_raw = pd.read_excel(
            ruta_excel,
            sheet_name=sheet_name,
            header=None,
            dtype=str
        ).fillna("")

        fila_header = detector_header(df_raw)

        if fila_header is None:
            print(f"⚠ No se detectó encabezado válido en hoja {sheet_name} de {os.path.basename(ruta_excel)}")
            return pd.DataFrame()

        df = pd.read_excel(
            ruta_excel,
            sheet_name=sheet_name,
            header=fila_header,
            dtype=str
        ).fillna("")

        df.columns = [str(c).strip() for c in df.columns]
        df["__archivo_origen"] = os.path.basename(ruta_excel)
        df["__hoja_origen"] = sheet_name

        return df

    except Exception as e:
        print(f"✗ Error leyendo hoja {sheet_name} en {os.path.basename(ruta_excel)}: {e}")
        return pd.DataFrame()


def leer_hojas_mes_eventos(ruta_excel):
    try:
        xls = pd.ExcelFile(ruta_excel)
    except Exception as e:
        print(f"✗ Error abriendo Excel de eventos {os.path.basename(ruta_excel)}: {e}")
        return {}

    hojas_mes = buscar_hojas_mes(xls.sheet_names)
    resultado = {}

    for hoja in hojas_mes:
        df = leer_hoja_con_header_detectado(
            ruta_excel,
            hoja,
            detectar_fila_header_eventos
        )
        if not df.empty:
            resultado[hoja] = df

    return resultado


def leer_hoja_codigo_marca(ruta_excel):
    try:
        xls = pd.ExcelFile(ruta_excel)
        hoja_objetivo = None

        for hoja in xls.sheet_names:
            if normalizar_columna(hoja) == "codigo marca":
                hoja_objetivo = hoja
                break

        if hoja_objetivo is None:
            print(f"⚠ No existe hoja CODIGO-MARCA en {os.path.basename(ruta_excel)}")
            return pd.DataFrame()

        return leer_hoja_con_header_detectado(
            ruta_excel,
            hoja_objetivo,
            detectar_fila_header_codigo_marca
        )

    except Exception as e:
        print(f"✗ Error leyendo CODIGO-MARCA en {os.path.basename(ruta_excel)}: {e}")
        return pd.DataFrame()


# ============================================================
# DETECCIÓN DE COLUMNAS
# ============================================================

def detectar_columnas_eventos(df):
    columnas = list(df.columns)

    resultado = {
        "rc": None,
        "numero_cam": None,
        "local": None,
        "fecha_inicio": None,
        "fecha_fin": None,
        "marca": None,
        "descuento": None,
        "personal_apoyo": None,
        "tipo_evento": None,
        "id_evento": None,
        "id_evento_respaldo": None,
        "rut_personal": None,
        "evento_cerrado_con_dermo": None,
        "financiamiento": None,
        "facturar_a": None,
        "tipo_pago": None,
        "lista_productos": None,
        "lista_locales": None,
    }

    for c in columnas:
        c_norm = normalizar_columna(c)

        if resultado["rc"] is None and c_norm == "rc":
            resultado["rc"] = c

        if resultado["numero_cam"] is None and (
            c_norm == "n cam"
            or c_norm == "numero cam"
            or c_norm == "n camana"
            or "cam" in c_norm
        ):
            resultado["numero_cam"] = c

        if resultado["local"] is None and c_norm == "local":
            resultado["local"] = c

        if resultado["fecha_inicio"] is None and (
            ("fecha" in c_norm and "inicio" in c_norm)
            or "feha de inicio evento" in c_norm
            or "fecha de inicio evento" in c_norm
        ):
            resultado["fecha_inicio"] = c

        if resultado["fecha_fin"] is None and (
            ("fecha" in c_norm and "termino" in c_norm)
            or ("fecha" in c_norm and "fin" in c_norm)
            or "feha termino evento" in c_norm
            or "fecha termino evento" in c_norm
        ):
            resultado["fecha_fin"] = c

        if resultado["marca"] is None and c_norm == "marca":
            resultado["marca"] = c

        if resultado["descuento"] is None and "descuento" in c_norm:
            resultado["descuento"] = c

        if resultado["personal_apoyo"] is None and "personal de apoyo" in c_norm:
            resultado["personal_apoyo"] = c

        if resultado["tipo_evento"] is None and "tipo de evento" in c_norm:
            resultado["tipo_evento"] = c

        # PRIORIDAD: ID GEO primero
        if resultado["id_evento"] is None and (
            c_norm == "id geo"
            or "id geo" in c_norm
        ):
            resultado["id_evento"] = c

        # RESPALDO: ID solo si no hay ID GEO
        if resultado["id_evento_respaldo"] is None and c_norm == "id":
            resultado["id_evento_respaldo"] = c

        if resultado["rut_personal"] is None and "rut" in c_norm:
            resultado["rut_personal"] = c

        if resultado["evento_cerrado_con_dermo"] is None and "cerrado con dermo" in c_norm:
            resultado["evento_cerrado_con_dermo"] = c

        if resultado["financiamiento"] is None and "financiamiento" in c_norm:
            resultado["financiamiento"] = c

        if resultado["facturar_a"] is None and "facturar" in c_norm:
            resultado["facturar_a"] = c

        if resultado["tipo_pago"] is None and "tipo pago" in c_norm:
            resultado["tipo_pago"] = c

        if resultado["lista_productos"] is None and (
            "lista productos" in c_norm
            or "lista de productos" in c_norm
        ):
            resultado["lista_productos"] = c

        if resultado["lista_locales"] is None and (
            "lista local" in c_norm
            or "lista de local" in c_norm
            or "lista locales" in c_norm
            or "lista de locales" in c_norm
        ):
            resultado["lista_locales"] = c

    return resultado


def detectar_columnas_codigo_marca(df):
    columnas = list(df.columns)

    resultado = {
        "sku": None,
        "descripcion": None,
        "marca": None,
    }

    for c in columnas:
        c_norm = normalizar_columna(c)

        if resultado["sku"] is None and c_norm == "codigo":
            resultado["sku"] = c

        if resultado["descripcion"] is None and (
            "descripcion" in c_norm
            or "descriptor" in c_norm
            or "decriptor" in c_norm
            or "producto" in c_norm
            or "nombre" in c_norm
        ):
            resultado["descripcion"] = c

        if resultado["marca"] is None and c_norm == "marca":
            resultado["marca"] = c

    return resultado


# ============================================================
# EXTRACCIÓN DE CATÁLOGO MARCA -> SKUS
# ============================================================

def extraer_catalogo_marcas_skus(ruta_excel):
    df = leer_hoja_codigo_marca(ruta_excel)

    if df.empty:
        return {}

    cols = detectar_columnas_codigo_marca(df)

    col_sku = cols["sku"]
    col_desc = cols["descripcion"]
    col_marca = cols["marca"]

    if not col_sku or not col_marca:
        print(f"⚠ CODIGO-MARCA sin columnas clave en {os.path.basename(ruta_excel)}")
        return {}

    catalogo = {}

    for _, row in df.iterrows():
        sku = valor_row(row, col_sku)
        marca = valor_row(row, col_marca)
        descripcion = valor_row(row, col_desc)

        if not sku or not marca:
            continue

        marca_key = normalizar_columna(marca)

        if marca_key not in catalogo:
            catalogo[marca_key] = []

        catalogo[marca_key].append({
            "sku": sku,
            "descripcion": descripcion,
            "marca": marca,
        })

    return catalogo


# ============================================================
# EXTRACCIÓN DE EVENTOS
# ============================================================

def fila_evento_tiene_datos_utiles(row, cols):
    valores = [
        valor_row(row, cols["id_evento"]),
        valor_row(row, cols["id_evento_respaldo"]),
        valor_row(row, cols["marca"]),
        valor_row(row, cols["fecha_inicio"]),
        valor_row(row, cols["fecha_fin"]),
        valor_row(row, cols["local"]),
        valor_row(row, cols["lista_locales"]),
        valor_row(row, cols["lista_productos"]),
    ]
    return any(v for v in valores)


def fila_corresponde_a_rc(row, col_rc, rc_objetivo=None):
    if not rc_objetivo:
        return True

    valor_rc = valor_row(row, col_rc)
    return normalizar_comparable(valor_rc) == normalizar_comparable(rc_objetivo)


def obtener_id_evento(row, cols):
    # PRIORIDAD ABSOLUTA: ID GEO
    id_geo = valor_row(row, cols["id_evento"])
    if id_geo:
        return id_geo

    # RESPALDO: ID
    return valor_row(row, cols["id_evento_respaldo"])


def construir_registro_evento(row, cols, rc_objetivo=None):
    if not fila_evento_tiene_datos_utiles(row, cols):
        return None

    if not fila_corresponde_a_rc(row, cols["rc"], rc_objetivo):
        return None

    marca = valor_row(row, cols["marca"])
    id_evento = obtener_id_evento(row, cols)

    if not marca or not id_evento:
        return None

    return {
        "archivo_origen": normalizar_texto(row.get("__archivo_origen", "")),
        "hoja_origen": normalizar_texto(row.get("__hoja_origen", "")),
        "id_evento": id_evento,
        "rc": valor_row(row, cols["rc"]),
        "numero_cam": valor_row(row, cols["numero_cam"]),
        "local": valor_row(row, cols["local"]),
        "lista_locales": valor_row(row, cols["lista_locales"]),
        "lista_productos": valor_row(row, cols["lista_productos"]),
        "fecha_inicio": formatear_fecha_excel(valor_row(row, cols["fecha_inicio"])),
        "fecha_fin": formatear_fecha_excel(valor_row(row, cols["fecha_fin"])),
        "marca": marca,
        "descuento": valor_row(row, cols["descuento"]),
        "personal_apoyo": valor_row(row, cols["personal_apoyo"]),
        "rut_personal": valor_row(row, cols["rut_personal"]),
        "tipo_evento": valor_row(row, cols["tipo_evento"]),
        "evento_cerrado_con_dermo": valor_row(row, cols["evento_cerrado_con_dermo"]),
        "financiamiento": valor_row(row, cols["financiamiento"]),
        "facturar_a": valor_row(row, cols["facturar_a"]),
        "tipo_pago": valor_row(row, cols["tipo_pago"]),
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def extraer_eventos_desde_excel(ruta_excel, rc_objetivo=None):
    hojas = leer_hojas_mes_eventos(ruta_excel)

    eventos = []
    resumen = []

    for hoja, df in hojas.items():
        cols = detectar_columnas_eventos(df)
        registros_hoja = 0

        for _, row in df.iterrows():
            registro = construir_registro_evento(
                row,
                cols,
                rc_objetivo=rc_objetivo
            )
            if registro:
                eventos.append(registro)
                registros_hoja += 1

        resumen.append({
            "archivo": os.path.basename(ruta_excel),
            "hoja": hoja,
            "eventos_detectados": registros_hoja
        })

    return pd.DataFrame(eventos), resumen


# ============================================================
# EXPANSIÓN EVENTO -> SKU
# ============================================================

def expandir_eventos_a_skus(df_eventos, catalogo_marcas_skus):
    if df_eventos.empty:
        return pd.DataFrame()

    registros = []

    for _, row in df_eventos.iterrows():
        marca = normalizar_texto(row.get("marca", ""))
        marca_key = normalizar_columna(marca)

        productos_marca = catalogo_marcas_skus.get(marca_key, [])

        for prod in productos_marca:
            registros.append({
                "id_evento": normalizar_texto(row.get("id_evento", "")),
                "marca": marca,
                "sku": normalizar_texto(prod.get("sku", "")),
                "descripcion": normalizar_texto(prod.get("descripcion", "")),
                "archivo_origen": normalizar_texto(row.get("archivo_origen", "")),
                "hoja_origen": normalizar_texto(row.get("hoja_origen", "")),
                "fecha_inicio": normalizar_texto(row.get("fecha_inicio", "")),
                "fecha_fin": normalizar_texto(row.get("fecha_fin", "")),
                "local": normalizar_texto(row.get("local", "")),
                "fecha_carga": normalizar_texto(row.get("fecha_carga", "")),
            })

    if not registros:
        return pd.DataFrame()

    return pd.DataFrame(registros)


# ============================================================
# PROCESO PRINCIPAL
# ============================================================

def procesar_excel_eventos(ruta_excel, rc_objetivo=None):
    """
    Procesa un Excel tipo EVENTOS y devuelve:
    - df_eventos
    - df_evento_skus
    - resumen debug

    Si rc_objetivo viene informado, solo carga campañas cuyo RC coincida.
    """
    df_eventos, resumen_hojas = extraer_eventos_desde_excel(
        ruta_excel,
        rc_objetivo=rc_objetivo
    )
    catalogo = extraer_catalogo_marcas_skus(ruta_excel)
    df_evento_skus = expandir_eventos_a_skus(df_eventos, catalogo)

    if "EVENTOS" in os.path.basename(ruta_excel).upper():
        print("\n" + "=" * 80)
        print(f"DEBUG EVENTOS -> {os.path.basename(ruta_excel)}")
        print(f"RC objetivo: {rc_objetivo}")
        print(f"Eventos detectados: {len(df_eventos)}")
        print(f"Relaciones evento_sku detectadas: {len(df_evento_skus)}")
        if not df_eventos.empty:
            cols_debug = [
                "id_evento",
                "rc",
                "marca",
                "local",
                "fecha_inicio",
                "fecha_fin",
                "lista_productos",
            ]
            cols_debug = [c for c in cols_debug if c in df_eventos.columns]
            print("\nPrimeros eventos:")
            print(df_eventos[cols_debug].head(10).to_string(index=False))
        if not df_evento_skus.empty:
            cols_debug = ["id_evento", "marca", "sku", "descripcion"]
            cols_debug = [c for c in cols_debug if c in df_evento_skus.columns]
            print("\nPrimeras relaciones evento_sku:")
            print(df_evento_skus[cols_debug].head(20).to_string(index=False))
        print("=" * 80 + "\n")

    return df_eventos, df_evento_skus, resumen_hojas


# ============================================================
# TEST LOCAL
# ============================================================

if __name__ == "__main__":
    ruta = "EVENTOS MARZO 2026 (1).xlsx"
    if os.path.exists(ruta):
        df_eventos, df_evento_skus, resumen = procesar_excel_eventos(
            ruta,
            rc_objetivo="FERNANDO"
        )
        print(f"Eventos: {len(df_eventos)}")
        print(f"Evento SKUs: {len(df_evento_skus)}")
        print(resumen)
    else:
        print("No se encontró el archivo de prueba.")
