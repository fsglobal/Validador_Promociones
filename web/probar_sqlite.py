from db import obtener_conexion


def mostrar_resumen():
    conn = obtener_conexion()
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM promociones").fetchone()[0]
    print(f"Total de registros en SQLite: {total}")

    print("\nPrimeros 20 registros:")
    filas = cur.execute("""
        SELECT archivo_origen, id_promocion, sku, descripcion
        FROM promociones
        LIMIT 20
    """).fetchall()

    for fila in filas:
        print(dict(fila))

    conn.close()


def buscar_por_sku(sku):
    conn = obtener_conexion()
    cur = conn.cursor()

    filas = cur.execute("""
        SELECT archivo_origen, id_promocion, sku, descripcion
        FROM promociones
        WHERE sku = ?
    """, (sku,)).fetchall()

    print(f"\nResultados para SKU = {sku}: {len(filas)}")
    for fila in filas:
        print(dict(fila))

    conn.close()


def buscar_por_id(id_promocion):
    conn = obtener_conexion()
    cur = conn.cursor()

    filas = cur.execute("""
        SELECT archivo_origen, id_promocion, sku, descripcion
        FROM promociones
        WHERE id_promocion = ?
    """, (id_promocion,)).fetchall()

    print(f"\nResultados para ID = {id_promocion}: {len(filas)}")
    for fila in filas:
        print(dict(fila))

    conn.close()


if __name__ == "__main__":
    mostrar_resumen()

    # pruebas concretas
    buscar_por_sku("53053")
    buscar_por_sku("287565")

    buscar_por_id("516348")
    buscar_por_id("516349")