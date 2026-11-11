"""Vectores de version y resolucion determinista para Taller 1.

Modulo puro: no accede a MongoDB ni modifica el inventario.
Las operaciones tentativas se registran por separado y solo se confirman
mediante el reconciliador, con lease/fencing y mayoria de MongoDB.
"""

from copy import deepcopy
from uuid import uuid4

DIMENSIONES = ("n1", "n2", "n3")
RECURSOS = ("cpu_cores", "memoria_mb", "ancho_banda_mbps")


def validar_vector(vector):
    if not isinstance(vector, dict) or set(vector) != set(DIMENSIONES):
        raise ValueError("Vector invalido: se requieren exactamente n1, n2 y n3")
    if any(type(vector[k]) is not int or vector[k] < 0 for k in DIMENSIONES):
        raise ValueError("Todos los contadores del vector deben ser enteros >= 0")
    return {k: vector[k] for k in DIMENSIONES}


def domina(a, b):
    """A domina a B si A >= B en todas las dimensiones y A > B en alguna."""
    a, b = validar_vector(a), validar_vector(b)
    return all(a[k] >= b[k] for k in DIMENSIONES) and any(a[k] > b[k] for k in DIMENSIONES)


def comparar_vectores(a, b):
    """Devuelve IGUALES, A_DOMINA, B_DOMINA o CONCURRENTES."""
    a, b = validar_vector(a), validar_vector(b)
    if a == b:
        return "IGUALES"
    if domina(a, b):
        return "A_DOMINA"
    if domina(b, a):
        return "B_DOMINA"
    return "CONCURRENTES"


def unir_vectores(*vectores):
    if not vectores:
        raise ValueError("Se requiere al menos un vector")
    datos = [validar_vector(v) for v in vectores]
    return {k: max(v[k] for v in datos) for k in DIMENSIONES}


def validar_recurso(recurso):
    partes = str(recurso).split(".")
    if len(partes) != 2 or partes[0] not in ("nodo_1", "nodo_2", "nodo_3") or partes[1] not in RECURSOS:
        raise ValueError("Recurso invalido: nodo_1|nodo_2|nodo_3.cpu_cores|memoria_mb|ancho_banda_mbps")
    return recurso


def crear_propuesta(nodo, recurso, valor, base_vector, valor_base, lamport, op_id=None):
    if type(nodo) is not int or nodo not in (1, 2, 3):
        raise ValueError("ID de nodo invalido")
    recurso = validar_recurso(recurso)
    if type(valor) is not int or valor < 0:
        raise ValueError("El valor propuesto debe ser un entero >= 0")
    if type(valor_base) is not int or valor_base < 0:
        raise ValueError("El valor base debe ser un entero >= 0")
    if type(lamport) is not int or lamport < 0:
        raise ValueError("Lamport debe ser entero >= 0")
    base = validar_vector(base_vector)
    nuevo = deepcopy(base)
    nuevo[f"n{nodo}"] += 1
    return {"op_id": op_id or uuid4().hex, "node": nodo, "resource": recurso,
            "value": valor, "base_value": valor_base,
            "base_vector": base, "vector": nuevo, "lamport": lamport,
            "confirmed": False}


def validar_propuesta(p):
    if not isinstance(p, dict) or not isinstance(p.get("op_id"), str) or not p["op_id"]:
        raise ValueError("Propuesta sin op_id")
    reconstruida = crear_propuesta(p.get("node"), p.get("resource"), p.get("value"),
                                   p.get("base_vector"), p.get("base_value"),
                                   p.get("lamport"), op_id=p["op_id"])
    if p.get("vector") != reconstruida["vector"]:
        raise ValueError("El vector de la propuesta no coincide con la version base")
    if p.get("confirmed") is not False:
        raise ValueError("Solo se admiten propuestas tentativas")
    return p


def resolver_par(a, b):
    """Compara dos propuestas y decide una ganadora sin escribir en MongoDB.

    En concurrencia: mayor Lamport; empate: menor ID de nodo.
    Si son del mismo nodo y mismo Lamport, desempate por op_id.
    """
    a, b = validar_propuesta(a), validar_propuesta(b)
    if a["op_id"] == b["op_id"]:
        raise ValueError("Se requieren dos operaciones diferentes")
    if a["resource"] != b["resource"]:
        raise ValueError("Las propuestas deben afectar el mismo recurso")
    if a["base_vector"] != b["base_vector"] or a["base_value"] != b["base_value"]:
        raise ValueError("Se requieren propuestas basadas en el mismo snapshot")
    relacion = comparar_vectores(a["vector"], b["vector"])
    if relacion == "A_DOMINA":
        ganador, perdedor, regla = a, b, "CAUSALIDAD_VECTOR"
    elif relacion == "B_DOMINA":
        ganador, perdedor, regla = b, a, "CAUSALIDAD_VECTOR"
    elif relacion == "CONCURRENTES":
        ganador, perdedor = sorted((a, b), key=lambda p: (-p["lamport"], p["node"], p["op_id"]))
        regla = "MAYOR_LAMPORT; EMPATE_MENOR_ID"
    else:
        raise ValueError("Dos versiones iguales no permiten resolver causalidad de cambios distintos")
    return {"relacion": relacion, "conflicto_detectado": relacion == "CONCURRENTES",
            "regla": regla, "ganador": ganador, "perdedor": perdedor,
            "vector_union": unir_vectores(a["vector"], b["vector"])}
