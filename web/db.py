import os
import sqlite3


def obtener_ruta_db():
    base_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(base_dir)

    data_dir = os.path.join(root_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    return os.path.join(data_dir, "promociones.db")


def obtener_conexion():
    ruta_db = obtener_ruta_db()
    conn = sqlite3.connect(ruta_db)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_db():
    conn = obtener_conexion()
    cur = conn.cursor()

    # ============================================================
    # TABLA PROMOCIONES
    # ============================================================

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promociones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archivo_origen TEXT,
            hoja_origen TEXT,
            id_promocion TEXT,
            sku TEXT,
            descripcion TEXT,
            precio TEXT,
            porcentaje TEXT,
            fecha_inicio TEXT,
            fecha_fin TEXT,
            mecanica TEXT,
            fecha_carga TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_promociones_sku
        ON promociones (sku)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_promociones_id_promocion
        ON promociones (id_promocion)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_promociones_archivo_origen
        ON promociones (archivo_origen)
    """)

    # ============================================================
    # TABLA EVENTOS
    # Cabecera de campañas/eventos
    # ============================================================

    cur.execute("""
        CREATE TABLE IF NOT EXISTS eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archivo_origen TEXT,
            hoja_origen TEXT,
            id_evento TEXT,
            rc TEXT,
            numero_cam TEXT,
            local TEXT,
            lista_locales TEXT,
            lista_productos TEXT,
            fecha_inicio TEXT,
            fecha_fin TEXT,
            marca TEXT,
            descuento TEXT,
            personal_apoyo TEXT,
            rut_personal TEXT,
            tipo_evento TEXT,
            evento_cerrado_con_dermo TEXT,
            financiamiento TEXT,
            facturar_a TEXT,
            tipo_pago TEXT,
            fecha_carga TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_id_evento
        ON eventos (id_evento)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_marca
        ON eventos (marca)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_local
        ON eventos (local)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_fecha_inicio
        ON eventos (fecha_inicio)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_fecha_fin
        ON eventos (fecha_fin)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_eventos_archivo_origen
        ON eventos (archivo_origen)
    """)

    # ============================================================
    # TABLA EVENTO_SKUS
    # Relación exacta evento <-> SKU
    # ============================================================

    cur.execute("""
        CREATE TABLE IF NOT EXISTS evento_skus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_evento TEXT,
            marca TEXT,
            sku TEXT,
            descripcion TEXT,
            archivo_origen TEXT,
            hoja_origen TEXT,
            fecha_inicio TEXT,
            fecha_fin TEXT,
            local TEXT,
            fecha_carga TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_evento_skus_id_evento
        ON evento_skus (id_evento)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_evento_skus_sku
        ON evento_skus (sku)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_evento_skus_marca
        ON evento_skus (marca)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_evento_skus_local
        ON evento_skus (local)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_evento_skus_archivo_origen
        ON evento_skus (archivo_origen)
    """)

    # ============================================================
    # TABLA ARCHIVOS IMPORTADOS
    # Preparada para control futuro de reprocesos
    # ============================================================

    cur.execute("""
        CREATE TABLE IF NOT EXISTS archivos_importados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            archivo_origen TEXT NOT NULL UNIQUE,
            ruta_archivo TEXT,
            tipo_archivo TEXT,
            fecha_modificacion REAL,
            tamano_bytes INTEGER,
            fecha_importacion TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_archivos_importados_archivo
        ON archivos_importados (archivo_origen)
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    inicializar_db()
    print("Base de datos SQLite inicializada correctamente.")
    print(f"Ruta DB: {obtener_ruta_db()}")
