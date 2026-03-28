from flask import Blueprint, render_template, request

from db import obtener_conexion


buscar_sqlite_bp = Blueprint("buscar_sqlite", __name__)


# ============================================================
# CONEXION / HELPERS
# ============================================================


def get_connection():
    return obtener_conexion()



def normalizar_texto(valor):
    if valor is None:
        return ""
    return str(valor).strip()



def es_numero(valor):
    return normalizar_texto(valor).isdigit()



def rows_to_dicts(rows):
    return [dict(r) for r in rows]



def _deduplicar_lista_registros(registros):
    vistos = set()
    salida = []

    for r in registros:
        clave = (
            normalizar_texto(r.get("tipo_registro")),
            normalizar_texto(r.get("tabla_origen")),
            normalizar_texto(r.get("archivo_origen")),
            normalizar_texto(r.get("hoja_origen")),
            normalizar_texto(r.get("id_promocion")),
            normalizar_texto(r.get("id_evento")),
            normalizar_texto(r.get("sku")),
            normalizar_texto(r.get("local")),
            normalizar_texto(r.get("fecha_inicio")),
            normalizar_texto(r.get("fecha_fin")),
        )
        if clave in vistos:
            continue
        vistos.add(clave)
        salida.append(r)

    return salida



def _ordenar_resultados(registros):
    return sorted(
        registros,
        key=lambda r: (
            0 if normalizar_texto(r.get("tipo_registro")) == "PROMOCION" else 1,
            normalizar_texto(r.get("id_promocion")) or normalizar_texto(r.get("id_evento")),
            normalizar_texto(r.get("sku")),
            normalizar_texto(r.get("local")),
        ),
    )


# ============================================================
# NORMALIZADORES DE SALIDA
# ============================================================


def _fila_promocion_a_resultado(row):
    return {
        "tipo_registro": "PROMOCION",
        "tabla_origen": "promociones",
        "archivo_origen": normalizar_texto(row.get("archivo_origen")),
        "hoja_origen": normalizar_texto(row.get("hoja_origen")),
        "id_promocion": normalizar_texto(row.get("id_promocion")),
        "id_evento": "",
        "sku": normalizar_texto(row.get("sku")),
        "descripcion": normalizar_texto(row.get("descripcion")),
        "marca": "",
        "local": "",
        "lista_locales": "",
        "lista_productos": "",
        "fecha_inicio": normalizar_texto(row.get("fecha_inicio")),
        "fecha_fin": normalizar_texto(row.get("fecha_fin")),
        "mecanica": normalizar_texto(row.get("mecanica")),
        "precio": normalizar_texto(row.get("precio")),
        "porcentaje": normalizar_texto(row.get("porcentaje")),
        "descuento": "",
        "rc": "",
        "numero_cam": "",
        "personal_apoyo": "",
        "rut_personal": "",
        "tipo_evento": "",
        "evento_cerrado_con_dermo": "",
        "financiamiento": "",
        "facturar_a": "",
        "tipo_pago": "",
        "fecha_carga": normalizar_texto(row.get("fecha_carga")),
    }



def _fila_evento_a_resultado(row):
    return {
        "tipo_registro": "EVENTO",
        "tabla_origen": "eventos",
        "archivo_origen": normalizar_texto(row.get("archivo_origen")),
        "hoja_origen": normalizar_texto(row.get("hoja_origen")),
        "id_promocion": "",
        "id_evento": normalizar_texto(row.get("id_evento")),
        "sku": normalizar_texto(row.get("sku")),
        "descripcion": normalizar_texto(row.get("descripcion")),
        "marca": normalizar_texto(row.get("marca")),
        "local": normalizar_texto(row.get("local")),
        "lista_locales": normalizar_texto(row.get("lista_locales")),
        "lista_productos": normalizar_texto(row.get("lista_productos")),
        "fecha_inicio": normalizar_texto(row.get("fecha_inicio")),
        "fecha_fin": normalizar_texto(row.get("fecha_fin")),
        "mecanica": "",
        "precio": "",
        "porcentaje": "",
        "descuento": normalizar_texto(row.get("descuento")),
        "rc": normalizar_texto(row.get("rc")),
        "numero_cam": normalizar_texto(row.get("numero_cam")),
        "personal_apoyo": normalizar_texto(row.get("personal_apoyo")),
        "rut_personal": normalizar_texto(row.get("rut_personal")),
        "tipo_evento": normalizar_texto(row.get("tipo_evento")),
        "evento_cerrado_con_dermo": normalizar_texto(row.get("evento_cerrado_con_dermo")),
        "financiamiento": normalizar_texto(row.get("financiamiento")),
        "facturar_a": normalizar_texto(row.get("facturar_a")),
        "tipo_pago": normalizar_texto(row.get("tipo_pago")),
        "fecha_carga": normalizar_texto(row.get("fecha_carga")),
    }


# ============================================================
# CONSULTAS BASE
# ============================================================


def _consultar_promociones_por_id(cur, id_promocion):
    filas = cur.execute(
        """
        SELECT *
        FROM promociones
        WHERE TRIM(COALESCE(id_promocion, '')) = TRIM(COALESCE(?, ''))
        ORDER BY archivo_origen, hoja_origen, sku
        """,
        (normalizar_texto(id_promocion),),
    ).fetchall()
    return [_fila_promocion_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_promociones_por_sku(cur, sku):
    filas = cur.execute(
        """
        SELECT *
        FROM promociones
        WHERE TRIM(COALESCE(sku, '')) = TRIM(COALESCE(?, ''))
        ORDER BY archivo_origen, hoja_origen, id_promocion
        """,
        (normalizar_texto(sku),),
    ).fetchall()
    return [_fila_promocion_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_eventos_por_id_evento(cur, id_evento):
    filas = cur.execute(
        """
        SELECT
            e.*,
            '' AS sku,
            '' AS descripcion
        FROM eventos e
        WHERE TRIM(COALESCE(e.id_evento, '')) = TRIM(COALESCE(?, ''))
        ORDER BY e.archivo_origen, e.hoja_origen, e.local
        """,
        (normalizar_texto(id_evento),),
    ).fetchall()
    return [_fila_evento_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_evento_skus_por_id_evento(cur, id_evento):
    filas = cur.execute(
        """
        SELECT
            e.archivo_origen,
            e.hoja_origen,
            e.id_evento,
            e.rc,
            e.numero_cam,
            COALESCE(es.local, e.local) AS local,
            e.lista_locales,
            e.lista_productos,
            COALESCE(es.fecha_inicio, e.fecha_inicio) AS fecha_inicio,
            COALESCE(es.fecha_fin, e.fecha_fin) AS fecha_fin,
            COALESCE(es.marca, e.marca) AS marca,
            e.descuento,
            e.personal_apoyo,
            e.rut_personal,
            e.tipo_evento,
            e.evento_cerrado_con_dermo,
            e.financiamiento,
            e.facturar_a,
            e.tipo_pago,
            e.fecha_carga,
            es.sku,
            es.descripcion
        FROM evento_skus es
        LEFT JOIN eventos e
            ON TRIM(COALESCE(e.id_evento, '')) = TRIM(COALESCE(es.id_evento, ''))
           AND TRIM(COALESCE(e.local, '')) = TRIM(COALESCE(es.local, ''))
           AND TRIM(COALESCE(e.fecha_inicio, '')) = TRIM(COALESCE(es.fecha_inicio, ''))
           AND TRIM(COALESCE(e.fecha_fin, '')) = TRIM(COALESCE(es.fecha_fin, ''))
        WHERE TRIM(COALESCE(es.id_evento, '')) = TRIM(COALESCE(?, ''))
        ORDER BY es.archivo_origen, es.hoja_origen, es.local, es.sku
        """,
        (normalizar_texto(id_evento),),
    ).fetchall()
    return [_fila_evento_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_evento_skus_por_sku(cur, sku):
    filas = cur.execute(
        """
        SELECT
            e.archivo_origen,
            e.hoja_origen,
            e.id_evento,
            e.rc,
            e.numero_cam,
            COALESCE(es.local, e.local) AS local,
            e.lista_locales,
            e.lista_productos,
            COALESCE(es.fecha_inicio, e.fecha_inicio) AS fecha_inicio,
            COALESCE(es.fecha_fin, e.fecha_fin) AS fecha_fin,
            COALESCE(es.marca, e.marca) AS marca,
            e.descuento,
            e.personal_apoyo,
            e.rut_personal,
            e.tipo_evento,
            e.evento_cerrado_con_dermo,
            e.financiamiento,
            e.facturar_a,
            e.tipo_pago,
            e.fecha_carga,
            es.sku,
            es.descripcion
        FROM evento_skus es
        LEFT JOIN eventos e
            ON TRIM(COALESCE(e.id_evento, '')) = TRIM(COALESCE(es.id_evento, ''))
           AND TRIM(COALESCE(e.local, '')) = TRIM(COALESCE(es.local, ''))
           AND TRIM(COALESCE(e.fecha_inicio, '')) = TRIM(COALESCE(es.fecha_inicio, ''))
           AND TRIM(COALESCE(e.fecha_fin, '')) = TRIM(COALESCE(es.fecha_fin, ''))
        WHERE TRIM(COALESCE(es.sku, '')) = TRIM(COALESCE(?, ''))
        ORDER BY es.archivo_origen, es.hoja_origen, es.local, es.id_evento
        """,
        (normalizar_texto(sku),),
    ).fetchall()
    return [_fila_evento_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_eventos_por_local(cur, local):
    filas = cur.execute(
        """
        SELECT
            e.*,
            '' AS sku,
            '' AS descripcion
        FROM eventos e
        WHERE TRIM(COALESCE(e.local, '')) = TRIM(COALESCE(?, ''))
        ORDER BY e.fecha_inicio, e.id_evento, e.local
        """,
        (normalizar_texto(local),),
    ).fetchall()
    return [_fila_evento_a_resultado(f) for f in rows_to_dicts(filas)]



def _consultar_evento_skus_por_local(cur, local):
    filas = cur.execute(
        """
        SELECT
            e.archivo_origen,
            e.hoja_origen,
            e.id_evento,
            e.rc,
            e.numero_cam,
            COALESCE(es.local, e.local) AS local,
            e.lista_locales,
            e.lista_productos,
            COALESCE(es.fecha_inicio, e.fecha_inicio) AS fecha_inicio,
            COALESCE(es.fecha_fin, e.fecha_fin) AS fecha_fin,
            COALESCE(es.marca, e.marca) AS marca,
            e.descuento,
            e.personal_apoyo,
            e.rut_personal,
            e.tipo_evento,
            e.evento_cerrado_con_dermo,
            e.financiamiento,
            e.facturar_a,
            e.tipo_pago,
            e.fecha_carga,
            es.sku,
            es.descripcion
        FROM evento_skus es
        LEFT JOIN eventos e
            ON TRIM(COALESCE(e.id_evento, '')) = TRIM(COALESCE(es.id_evento, ''))
           AND TRIM(COALESCE(e.local, '')) = TRIM(COALESCE(es.local, ''))
           AND TRIM(COALESCE(e.fecha_inicio, '')) = TRIM(COALESCE(es.fecha_inicio, ''))
           AND TRIM(COALESCE(e.fecha_fin, '')) = TRIM(COALESCE(es.fecha_fin, ''))
        WHERE TRIM(COALESCE(es.local, '')) = TRIM(COALESCE(?, ''))
        ORDER BY es.fecha_inicio, es.id_evento, es.sku
        """,
        (normalizar_texto(local),),
    ).fetchall()
    return [_fila_evento_a_resultado(f) for f in rows_to_dicts(filas)]


# ============================================================
# BUSQUEDAS COMPATIBLES CON GESTOR / REPOSITORIO
# ============================================================


def buscar_por_id(id_buscado):
    conn = get_connection()
    try:
        cur = conn.cursor()
        resultados = []
        resultados.extend(_consultar_promociones_por_id(cur, id_buscado))
        resultados.extend(_consultar_eventos_por_id_evento(cur, id_buscado))
        resultados.extend(_consultar_evento_skus_por_id_evento(cur, id_buscado))
        return _ordenar_resultados(_deduplicar_lista_registros(resultados))
    finally:
        conn.close()



def buscar_por_sku(sku):
    conn = get_connection()
    try:
        cur = conn.cursor()
        resultados = []
        resultados.extend(_consultar_promociones_por_sku(cur, sku))
        resultados.extend(_consultar_evento_skus_por_sku(cur, sku))
        return _ordenar_resultados(_deduplicar_lista_registros(resultados))
    finally:
        conn.close()



def buscar_por_local(local):
    conn = get_connection()
    try:
        cur = conn.cursor()
        resultados = []
        resultados.extend(_consultar_eventos_por_local(cur, local))
        resultados.extend(_consultar_evento_skus_por_local(cur, local))
        return _ordenar_resultados(_deduplicar_lista_registros(resultados))
    finally:
        conn.close()



def buscador_unificado(q, modo="auto"):
    q = normalizar_texto(q)
    modo = normalizar_texto(modo).lower()

    if not q:
        return []

    if modo == "id":
        return buscar_por_id(q)

    if modo == "sku":
        return buscar_por_sku(q)

    if modo == "local":
        return buscar_por_local(q)

    if es_numero(q):
        resultados = buscar_por_id(q)
        if resultados:
            return resultados
        return buscar_por_sku(q)

    return buscar_por_local(q)



def buscar_promociones_sqlite(sku_buscado=None, promo_id_buscado=None, local_buscado=None, **kwargs):
    """
    Función de compatibilidad con gestor.py.

    Acepta los nombres de parámetros que ya usa el proyecto:
    - sku_buscado
    - promo_id_buscado
    - local_buscado

    También ignora kwargs extra para no romper futuras llamadas.
    """
    sku_buscado = normalizar_texto(sku_buscado)
    promo_id_buscado = normalizar_texto(promo_id_buscado)
    local_buscado = normalizar_texto(local_buscado)

    if promo_id_buscado and sku_buscado:
        resultados = buscar_por_id(promo_id_buscado)
        resultados = [
            r for r in resultados
            if normalizar_texto(r.get("sku")) == sku_buscado or not normalizar_texto(r.get("sku"))
        ]
        return _ordenar_resultados(_deduplicar_lista_registros(resultados))

    if promo_id_buscado:
        return buscar_por_id(promo_id_buscado)

    if sku_buscado:
        return buscar_por_sku(sku_buscado)

    if local_buscado:
        return buscar_por_local(local_buscado)

    return []


# ============================================================
# RUTA FLASK OPCIONAL PARA PRUEBAS
# ============================================================


@buscar_sqlite_bp.route("/buscar_sqlite", methods=["GET", "POST"])
def buscar_sqlite():
    resultados = None
    q = ""
    modo = "auto"

    if request.method == "POST":
        q = request.form.get("q", "").strip()
        modo = request.form.get("modo", "auto").strip().lower()
        resultados = buscador_unificado(q, modo)

    return render_template(
        "buscar_sqlite.html",
        resultados=resultados,
        q=q,
        modo=modo,
    )
