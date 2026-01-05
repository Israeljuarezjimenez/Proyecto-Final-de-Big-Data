def normalizar_mes(mes):
    try:
        mes_int = int(mes)
    except (TypeError, ValueError):
        raise ValueError("Mes invalido")

    if mes_int < 1 or mes_int > 12:
        raise ValueError("Mes fuera de rango (1-12)")
    return f"{mes_int:02d}"

def resolver_meses(mes=None, meses=None, trimestre=None):
    opciones = [bool(mes), bool(meses), bool(trimestre)]
    if sum(opciones) != 1:
        raise ValueError("Debes indicar solo una opcion: --month, --months o --quarter")

    if trimestre is not None:
        try:
            trimestre_int = int(trimestre)
        except (TypeError, ValueError):
            raise ValueError("Trimestre invalido")
        mapa = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
        if trimestre_int not in mapa:
            raise ValueError("Trimestre fuera de rango (1-4)")
        return [normalizar_mes(m) for m in mapa[trimestre_int]]

    if meses:
        partes = [p.strip() for p in str(meses).split(",") if p.strip()]
        if not partes:
            raise ValueError("Lista de meses vacia")
        return [normalizar_mes(p) for p in partes]

    return [normalizar_mes(mes)]
