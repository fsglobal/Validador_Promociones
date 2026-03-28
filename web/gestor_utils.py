import os
import re
import unicodedata
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
    texto = " ".join(texto.split())
    return texto


def valor_columna(row, columna):
    if not columna:
        return ""
    return normalizar_texto(row.get(columna, ""))


def detectar_fila_header(df_raw, max_filas=10):
    """
    Busca en las primeras filas una que parezca encabezado real.
    """
    for i in range(min(max_filas, len(df_raw))):
        fila_texto = " | ".join(df_raw.iloc[i].astype(str).str.upper().tolist())

        if (
            "GEOCOM" in fila_texto
            or "SKU" in fila_texto
            or "ALTERNATIVO" in fila_texto
            or "PROMO" in fila_texto
            or "FECHA INICIO" in fila_texto
            or "FECHA FIN" in fila_texto
            or "CODIGO" in fila_texto
            or "PRODUCTO" in fila_texto
            or "DESCRIP" in fila_texto
        ):
            return i

    return None


# ============================================================
# LECTURA EXCEL - HOJA COMPLETAR
# ============================================================

def leer_hoja_completar_gestor(ruta_excel):
    """
    Lee la hoja COMPLETAR del Excel detectando automáticamente
    la fila real de encabezados.
    """
    try:
        xls = pd.ExcelFile(ruta_excel)
        hojas = xls.sheet_names

        hoja_objetivo = None
        for hoja in hojas:
            if hoja.strip().upper() == "COMPLETAR":
                hoja_objetivo = hoja
                break

        if hoja_objetivo is None:
            print(f"⚠ No existe hoja COMPLETAR en {os.path.basename(ruta_excel)}")
            return pd.DataFrame()

        df_raw = pd.read_excel(
            ruta_excel,
            sheet_name=hoja_objetivo,
            header=None,
            dtype=str
        ).fillna("")

        fila_header = detectar_fila_header(df_raw)

        if fila_header is None:
            print(f"⚠ No se detectó encabezado válido en {os.path.basename(ruta_excel)}")
            return pd.DataFrame()

        df = pd.read_excel(
            ruta_excel,
            sheet_name=hoja_objetivo,
            header=fila_header,
            dtype=str
        ).fillna("")

        df.columns = [str(c).strip() for c in df.columns]
        df["__archivo_origen"] = os.path.basename(ruta_excel)
        df["__hoja_origen"] = hoja_objetivo

        return df

    except Exception as e:
        print(f"✗ Error leyendo COMPLETAR en {os.path.basename(ruta_excel)}: {e}")
        return pd.DataFrame()


# ============================================================
# HEURÍSTICAS DE COLUMNAS
# ============================================================

def parece_codigo_o_numero(valor):
    """
    Devuelve True si el valor parece un código/SKU/EAN y no una descripción.
    """
    texto = normalizar_texto(valor)
    if not texto:
        return False

    texto_limpio = (
        texto.replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .replace(".", "")
        .replace("/", "")
    )

    if texto_limpio.isdigit():
        return True

    if re.fullmatch(r"[A-Za-z]*\d+[A-Za-z\d]*", texto_limpio):
        letras = sum(1 for c in texto_limpio if c.isalpha())
        digitos = sum(1 for c in texto_limpio if c.isdigit())
        if digitos >= letras:
            return True

    return False


def parece_texto_descriptivo(valor):
    """
    Devuelve True si el valor parece descripción de producto.
    """
    texto = normalizar_texto(valor)
    if not texto:
        return False

    if parece_codigo_o_numero(texto):
        return False

    letras = sum(1 for c in texto if c.isalpha())
    espacios = texto.count(" ")
    longitud = len(texto)

    if letras >= 3 and (espacios >= 1 or longitud >= 8):
        return True

    return False


def es_columna_probable_descripcion_por_nombre(nombre_columna):
    """
    Determina si una columna parece descriptiva por su encabezado.
    """
    c_norm = normalizar_columna(nombre_columna)

    patrones_validos_fuertes = [
        "descripcion",
        "descriptor",
        "descripcion producto",
        "nombre producto",
        "nombre del producto",
        "detalle producto",
        "desc producto",
    ]

    patrones_validos_debiles = [
        "producto",
        "articulo",
        "item",
        "detalle",
        "nombre",
    ]

    patrones_invalidos = [
        "sku",
        "ean",
        "geocom",
        "codigo",
        "cod producto",
        "codigo producto",
        "codigo pro",
        "alternativo",
        "id promo",
        "id promocion",
        "fecha",
        "precio",
        "monto",
        "pvp",
        "porcentaje",
        "descuento",
        "mecanica",
        "tipo promo",
        "tipo promocion",
    ]

    if any(p in c_norm for p in patrones_invalidos):
        return False, 0

    if any(p in c_norm for p in patrones_validos_fuertes):
        return True, 3

    if any(p in c_norm for p in patrones_validos_debiles):
        return True, 1

    return False, 0


def puntuar_columna_descripcion(df, columna, max_muestra=20):
    """
    Puntúa una columna candidata a descripción usando:
    - nombre del encabezado
    - contenido real de las filas
    """
    es_candidata, prioridad_nombre = es_columna_probable_descripcion_por_nombre(columna)
    if not es_candidata:
        return -1

    serie = df[columna].fillna("").astype(str)
    muestra = [normalizar_texto(v) for v in serie.tolist() if normalizar_texto(v)]

    if not muestra:
        return prioridad_nombre

    muestra = muestra[:max_muestra]

    descriptivos = sum(1 for v in muestra if parece_texto_descriptivo(v))
    codigos = sum(1 for v in muestra if parece_codigo_o_numero(v))

    score = (prioridad_nombre * 100) + (descriptivos * 10) - (codigos * 8)

    return score


# ============================================================
# DETECCIÓN DE COLUMNAS
# ============================================================

def detectar_columna_descripcion(df):
    mejor_columna = None
    mejor_score = -1

    for c in df.columns:
        score = puntuar_columna_descripcion(df, c)
        if score > mejor_score:
            mejor_score = score
            mejor_columna = c

    if mejor_score <= 0:
        return None

    return mejor_columna


def detectar_columnas_repositorio(df):
    """
    Detecta columnas relevantes para consolidar el repositorio.
    """
    columnas = list(df.columns)

    resultado = {
        "id_promocion": None,
        "sku": None,
        "fecha_inicio": None,
        "fecha_fin": None,
        "precio": None,
        "porcentaje": None,
        "descripcion": None,
        "mecanica": None,
    }

    for c in columnas:
        c_norm = normalizar_columna(c)

        if resultado["id_promocion"] is None and (
            "geocom" in c_norm
            or "id promo" in c_norm
            or "id promocion" in c_norm
        ):
            resultado["id_promocion"] = c

        if resultado["sku"] is None and (
            "sku" in c_norm
            or "alternativo" in c_norm
            or "ean" in c_norm
            or "codigo producto" in c_norm
            or "codigo pro" in c_norm
            or "cod producto" in c_norm
        ):
            resultado["sku"] = c

        if resultado["fecha_inicio"] is None and (
            ("fecha" in c_norm and "inicio" in c_norm)
            or c_norm == "f inicio"
        ):
            resultado["fecha_inicio"] = c

        if resultado["fecha_fin"] is None and (
            ("fecha" in c_norm and "fin" in c_norm)
            or ("fecha" in c_norm and "termino" in c_norm)
            or c_norm == "f termino"
        ):
            resultado["fecha_fin"] = c

        if resultado["precio"] is None and (
            "precio" in c_norm
            or "monto" in c_norm
            or "pvp" in c_norm
        ):
            resultado["precio"] = c

        if resultado["porcentaje"] is None and (
            "porcentaje" in c_norm
            or "%" in c_norm
            or "descuento" in c_norm
        ):
            resultado["porcentaje"] = c

        if resultado["mecanica"] is None and (
            "mecanica" in c_norm
            or "tipo promo" in c_norm
            or "tipo promocion" in c_norm
            or "tipo de descuento" in c_norm
        ):
            resultado["mecanica"] = c

    resultado["descripcion"] = detectar_columna_descripcion(df)

    return resultado


# ============================================================
# LIMPIEZA / NORMALIZACIÓN DE FILAS
# ============================================================

def fila_tiene_datos_utiles(id_promocion, sku, fecha_inicio, fecha_fin, precio, porcentaje):
    """
    Evita guardar filas totalmente vacías o basura.
    """
    valores = [id_promocion, sku, fecha_inicio, fecha_fin, precio, porcentaje]
    return any(normalizar_texto(v) for v in valores)


def construir_registro_repositorio(row, cols):
    id_promocion = valor_columna(row, cols["id_promocion"])
    sku = valor_columna(row, cols["sku"])
    fecha_inicio = valor_columna(row, cols["fecha_inicio"])
    fecha_fin = valor_columna(row, cols["fecha_fin"])
    precio = valor_columna(row, cols["precio"])
    porcentaje = valor_columna(row, cols["porcentaje"])
    descripcion = valor_columna(row, cols["descripcion"])
    mecanica = valor_columna(row, cols["mecanica"])

    if not fila_tiene_datos_utiles(id_promocion, sku, fecha_inicio, fecha_fin, precio, porcentaje):
        return None

    if descripcion and parece_codigo_o_numero(descripcion):
        descripcion = ""

    return {
        "fecha_carga": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "archivo_origen": normalizar_texto(row.get("__archivo_origen", "")),
        "hoja_origen": normalizar_texto(row.get("__hoja_origen", "")),
        "id_promocion": id_promocion,
        "sku": sku,
        "descripcion": descripcion,
        "precio": precio,
        "porcentaje": porcentaje,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "mecanica": mecanica,
    }


# ============================================================
# EXTRACCIÓN PARA REPOSITORIO
# ============================================================

def extraer_promos_para_repositorio(ruta_excel):
    """
    Devuelve un DataFrame estandarizado con promociones extraídas
    desde la hoja COMPLETAR.
    """
    df = leer_hoja_completar_gestor(ruta_excel)

    if df.empty:
        return pd.DataFrame()

    cols = detectar_columnas_repositorio(df)

    if not cols["id_promocion"] and not cols["sku"]:
        print(f"⚠ No se detectaron columnas útiles en {os.path.basename(ruta_excel)}")
        return pd.DataFrame()

    registros = []

    for _, row in df.iterrows():
        registro = construir_registro_repositorio(row, cols)
        if registro:
            registros.append(registro)

    if not registros:
        return pd.DataFrame()

    return pd.DataFrame(registros)


# ============================================================
# REPOSITORIO - PROCESO MASIVO
# ============================================================

def listar_excels_repositorio(carpeta_repositorio):
    if not os.path.exists(carpeta_repositorio):
        return []

    return sorted([
        os.path.join(carpeta_repositorio, f)
        for f in os.listdir(carpeta_repositorio)
        if f.lower().endswith(".xlsx")
    ])


def procesar_excels_repositorio(carpeta_repositorio):
    """
    Procesa todos los Excel de ExcelRepositorio y devuelve:
    - dataframe consolidado
    - resumen por archivo
    """
    archivos = listar_excels_repositorio(carpeta_repositorio)

    todos = []
    resumen = []

    for ruta_excel in archivos:
        nombre = os.path.basename(ruta_excel)

        try:
            df = extraer_promos_para_repositorio(ruta_excel)

            if "NEOLUCID" in nombre.upper():
                print("\n" + "=" * 80)
                print(f"DEBUG REPOSITORIO NEOLUCID -> {nombre}")
                if df.empty:
                    print("DataFrame vacío")
                else:
                    print(f"Filas extraídas: {len(df)}")
                    columnas_debug = ["archivo_origen", "id_promocion", "sku", "descripcion"]
                    columnas_debug = [c for c in columnas_debug if c in df.columns]
                    print(df[columnas_debug].head(30).to_string(index=False))
                print("=" * 80 + "\n")

            cantidad = 0 if df.empty else len(df)

            resumen.append({
                "archivo": nombre,
                "estado": "OK" if cantidad > 0 else "SIN DATOS",
                "registros": cantidad,
                "detalle": ""
            })

            if not df.empty:
                todos.append(df)

        except Exception as e:
            resumen.append({
                "archivo": nombre,
                "estado": "ERROR",
                "registros": 0,
                "detalle": str(e)
            })

    if todos:
        df_total = pd.concat(todos, ignore_index=True)
    else:
        df_total = pd.DataFrame(columns=[
            "fecha_carga",
            "archivo_origen",
            "hoja_origen",
            "id_promocion",
            "sku",
            "descripcion",
            "precio",
            "porcentaje",
            "fecha_inicio",
            "fecha_fin",
            "mecanica",
        ])

    return df_total, resumen


# ============================================================
# DEDUPLICACIÓN
# ============================================================

def deduplicar_repositorio(df):
    """
    Elimina duplicados manteniendo histórico por archivo.
    """
    if df.empty:
        return df.copy(), 0

    columnas_clave = [
        "archivo_origen",
        "id_promocion",
        "sku",
        "fecha_inicio",
        "fecha_fin",
        "precio",
        "porcentaje",
    ]

    df_limpio = df.copy()
    for col in columnas_clave:
        if col not in df_limpio.columns:
            df_limpio[col] = ""

    antes = len(df_limpio)
    df_limpio = df_limpio.drop_duplicates(subset=columnas_clave, keep="first").reset_index(drop=True)
    despues = len(df_limpio)

    return df_limpio, antes - despues
