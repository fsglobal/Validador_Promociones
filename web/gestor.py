import os
from flask import render_template, request

from buscar_sqlite import buscar_promociones_sqlite


def registrar_rutas_gestor(app):

    # ============================================================
    # PANTALLA PRINCIPAL DEL GESTOR
    # ============================================================

    @app.route("/gestor")
    def gestor():
        return render_template(
            "gestor.html",
            resultados=None,
            mensaje=""
        )

    # ============================================================
    # BUSCAR PROMOCIONES
    # ============================================================

    @app.route("/buscar_promos", methods=["POST"])
    def buscar_promos():

        sku_buscado = request.form.get("sku", "").strip()
        promo_id_buscado = request.form.get("promo_id", "").strip()

        print("\n" + "=" * 80)
        print("DEBUG GESTOR SQLITE")
        print(f"SKU buscado: [{sku_buscado}]")
        print(f"ID promo buscado: [{promo_id_buscado}]")

        # --------------------------------------------------------
        # VALIDAR QUE HAYA ALGO PARA BUSCAR
        # --------------------------------------------------------

        if not sku_buscado and not promo_id_buscado:

            print("No se ingresó ningún criterio de búsqueda.")
            print("=" * 80 + "\n")

            return render_template(
                "gestor.html",
                resultados=None,
                mensaje="Debe ingresar un SKU o un ID de promoción para buscar."
            )

        # --------------------------------------------------------
        # CONSULTAR SQLITE
        # --------------------------------------------------------

        resultados = buscar_promociones_sqlite(
            sku_buscado=sku_buscado,
            promo_id_buscado=promo_id_buscado
        )

        print(f"Resultados SQLite: {len(resultados)}")
        print("=" * 80 + "\n")

        # --------------------------------------------------------
        # MENSAJE SI NO HAY RESULTADOS
        # --------------------------------------------------------

        mensaje = ""

        if not resultados:
            mensaje = "No se encontraron promociones con esos criterios."

        # --------------------------------------------------------
        # DEVOLVER RESULTADO AL HTML
        # --------------------------------------------------------

        return render_template(
            "gestor.html",
            resultados=resultados,
            mensaje=mensaje
        )