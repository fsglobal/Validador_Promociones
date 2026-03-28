import re
from pathlib import Path

def parsear_listas_productos_export(export_path):
    """
    Devuelve:
    {
        "ACT NAV 25%": {"295429", "296972", ...}
    }
    """
    text = Path(export_path).read_text(encoding="utf-8", errors="ignore")

    pattern = re.compile(
        r"<uy\.com\.geocom\.geopromotion\.service\.list\.ProductListItem>.*?"
        r"<itemId>(\d+)</itemId>.*?"
        r"<listName>(.*?)</listName>",
        re.DOTALL
    )

    listas = {}

    for item_id, list_name in pattern.findall(text):
        list_name = list_name.strip()
        listas.setdefault(list_name, set()).add(item_id)

    return listas
