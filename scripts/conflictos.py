"""CLI de operaciones tentativas y reconciliacion del Taller 1.

Solo el comando 'reconcile' modifica MongoDB y requiere al lider Bully
legitimo, comprobando su lease dentro de una transaccion w=majority.
"""

import argparse
import hashlib
import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.conflictos import (crear_propuesta, resolver_par, unir_vectores,
                           validar_propuesta, validar_recurso, validar_vector)

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / "logs" / "conflictos"


def node_id_local():
    nombre = socket.gethostname().split(".", 1)[0]
    if nombre not in ("sdm-node1", "sdm-node2", "sdm-node3"):
        raise RuntimeError(f"Hostname no reconocido: {nombre}")
    return int(nombre[-1])


def consultar_bully():
    """Usa localhost; disponible aun con Tailscale aislado."""
    payload = {"type": "STATUS", "from": 0, "epoch": 0, "lamport": 0}
    with socket.create_connection(("127.0.0.1", 5050), timeout=5) as conn:
        conn.settimeout(8)
        conn.sendall((json.dumps(payload) + "\n").encode())
        respuesta = conn.makefile("r").readline()
    if not respuesta:
        raise RuntimeError("Bully no respondio al STATUS local")
    status = json.loads(respuesta)
    if status.get("type") != "STATUS" or status.get("from") != node_id_local():
        raise RuntimeError("El proceso local no devolvio un STATUS valido")
    return status


def conectar_mongo():
    from pymongo import MongoClient
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise RuntimeError("Falta MONGO_URI; ejecuta scripts/conflictos.sh")
    cli = MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=5000,
                      connectTimeoutMS=3000, socketTimeoutMS=10000,
                      appname="taller1sd-conflictos")
    db = cli.get_database("recursos_borde", read_concern=ReadConcern("majority"),
                          write_concern=WriteConcern("majority", wtimeout=10000))
    return cli, db


def imprimir(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def leer_json(archivo):
    return json.loads(Path(archivo).read_text(encoding="utf-8"))


def escribir_json(archivo, data):
    archivo = Path(archivo)
    archivo.parent.mkdir(parents=True, exist_ok=True)
    archivo.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return archivo


def snapshot(args):
    nodo = node_id_local()
    cli, db = conectar_mongo()
    try:
        inv = db.inventario.find_one({"_id": "inventario_global"},
                                     {"estado_recursos": 1, "vector_version": 1})
        if not inv:
            raise RuntimeError("inventario_global no existe")
        estado = inv.get("estado_recursos")
        if not isinstance(estado, dict):
            raise RuntimeError("estado_recursos invalido")
        v = validar_vector(inv.get("vector_version"))
        data = {"node": nodo, "timestamp": datetime.now(timezone.utc).isoformat(),
                "vector_version": v, "estado_recursos": estado}
        destino = escribir_json(LOCAL / f"snapshot_node{nodo}.json", data)
        imprimir({"resultado": "SNAPSHOT_GUARDADO", "node": nodo, "archivo": str(destino),
                 "vector_version": v})
    finally:
        cli.close()


def recurso_en(snapshot_data, nombre):
    nodo, campo = validar_recurso(nombre).split(".", 1)
    try:
        valor = snapshot_data["estado_recursos"][nodo][campo]
    except (KeyError, TypeError) as e:
        raise ValueError(f"Recurso inexistente en el snapshot: {nombre}") from e
    if type(valor) is not int:
        raise ValueError("El valor del recurso no es entero")
    return valor


def propose(args):
    nodo = node_id_local()
    archivo = LOCAL / f"snapshot_node{nodo}.json"
    if not archivo.exists():
        raise RuntimeError("Primero ejecuta: bash scripts/conflictos.sh snapshot")
    snap = leer_json(archivo)
    if snap.get("node") != nodo:
        raise RuntimeError("Snapshot de otro nodo")
    # El reloj procede del proceso Bully local. No se necesita comunicacion con Mongo.
    estado = consultar_bully()
    lamport = estado.get("lamport")
    if type(lamport) is not int:
        raise RuntimeError("El proceso Bully no devolvio un Lamport valido")
    p = crear_propuesta(nodo, args.resource, args.value, snap["vector_version"],
                        recurso_en(snap, args.resource), lamport)
    p["created_at"] = datetime.now(timezone.utc).isoformat()
    archivo = escribir_json(LOCAL / f"propuesta-node{nodo}-{p['op_id']}.json", p)
    imprimir({"resultado": "TENTATIVE_NOT_COMMITTED", "archivo": str(archivo),
             "propuesta": p})


def analizar(args):
    a, b = leer_json(args.a), leer_json(args.b)
    r = resolver_par(a, b)
    imprimir({"relacion": r["relacion"], "conflictos_detectados": int(r["conflicto_detectado"]),
             "regla": r["regla"], "recurso": r["ganador"]["resource"],
             "ganador": {k: r["ganador"][k] for k in ("op_id", "node", "value", "lamport", "vector")},
             "perdedor": {k: r["perdedor"][k] for k in ("op_id", "node", "value", "lamport", "vector")},
             "vector_union": r["vector_union"]})


def reconciliar(args):
    from pymongo import ReturnDocument
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import ReadPreference
    from pymongo.write_concern import WriteConcern

    node = node_id_local()
    status = consultar_bully()
    if status.get("state") != "LIDER" or status.get("leader") != node or status.get("can_assign") is not True:
        raise RuntimeError("Solo el lider Bully con lease valido puede reconciliar")
    a, b = validar_propuesta(leer_json(args.a)), validar_propuesta(leer_json(args.b))
    r = resolver_par(a, b)
    if not r["conflicto_detectado"]:
        raise RuntimeError("Este procedimiento solo resuelve conflictos concurrentes")
    conflict_id = hashlib.sha256("|".join(sorted((a["op_id"], b["op_id"]))).encode()).hexdigest()
    recurso = r["ganador"]["resource"]
    cli, db = conectar_mongo()

    try:
        lease_actual = db.coordinacion.find_one({"_id": "bully"})
        if (not lease_actual or lease_actual.get("owner") != node
                or lease_actual.get("epoch") != status["epoch"]
                or not lease_actual.get("token")):
            raise RuntimeError("No existe lease persistente vigente para este coordinador")
        token = lease_actual["token"]

        def transaccion(s):
            anterior = db.resoluciones_conflicto.find_one({"_id": conflict_id}, session=s)
            if anterior:
                return {"resultado": "YA_RESUELTO", "conflicto_id": conflict_id,
                        "ganador": anterior.get("ganador"), "vector_resuelto": anterior.get("vector_resuelto")}

            # Comprobar el lease dentro de la MISMA transaccion de inventario.
            now = datetime.now(timezone.utc)
            owner = db.coordinacion.find_one_and_update(
                {"_id": "bully", "owner": node, "epoch": status["epoch"],
                 "token": token, "lease_until": {"$gt": now}},
                {"$inc": {"operation_count": 1}},
                return_document=ReturnDocument.AFTER, session=s)
            if owner is None:
                raise RuntimeError("Lease perdido: reconciliacion RECHAZADA")
            inv = db.inventario.find_one({"_id": "inventario_global"}, session=s)
            if not inv:
                raise RuntimeError("No existe inventario_global")
            valor_actual = recurso_en(inv, recurso)
            if valor_actual != r["ganador"]["base_value"]:
                raise RuntimeError("El recurso cambio desde el snapshot; requiere revision manual")
            vactual = validar_vector(inv.get("vector_version"))
            # La reconciliacion es un nuevo evento causal del lider.
            vfinal = unir_vectores(vactual, r["vector_union"])
            vfinal[f"n{node}"] += 1
            db.inventario.update_one(
                {"_id": "inventario_global"},
                {"$set": {f"estado_recursos.{recurso}": r["ganador"]["value"],
                          "vector_version": vfinal}}, session=s)
            ganador = {k: r["ganador"][k] for k in ("op_id", "node", "value", "lamport", "vector")}
            perdedor = {k: r["perdedor"][k] for k in ("op_id", "node", "value", "lamport", "vector")}
            auditoria = {"_id": conflict_id, "recurso": recurso,
                         "relacion": r["relacion"], "regla": r["regla"],
                         "ganador": ganador, "perdedor": perdedor,
                         "base_vector": a["base_vector"],
                         "vector_resuelto": vfinal,
                         "coordinador": node, "epoch": status["epoch"],
                         "detectados": 1, "resueltos": 1,
                         "resuelto_en": now}
            db.resoluciones_conflicto.insert_one(auditoria, session=s)
            db.metricas_conflictos.update_one(
                {"_id": "taller1"},
                {"$inc": {"detectados": 1, "resueltos": 1}},
                upsert=True, session=s)
            return {"resultado": "CONFLICTO_RESUELTO", "conflicto_id": conflict_id,
                    "recurso": recurso, "ganador": ganador,
                    "perdedor": perdedor, "vector_resuelto": vfinal,
                    "coordinador": node, "epoch": status["epoch"]}

        with cli.start_session() as session:
            resultado = session.with_transaction(
                transaccion, read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority", wtimeout=10000),
                read_preference=ReadPreference.PRIMARY, max_commit_time_ms=8000)
        imprimir(resultado)
    finally:
        cli.close()


def metricas(args):
    cli, db = conectar_mongo()
    try:
        m = db.metricas_conflictos.find_one({"_id": "taller1"}) or {}
        registros = list(db.resoluciones_conflicto.find({},
                          {"recurso": 1, "coordinador": 1, "epoch": 1, "ganador": 1}).limit(20))
        imprimir({"conflictos_detectados": m.get("detectados", 0),
                 "conflictos_resueltos": m.get("resueltos", 0),
                 "registros_recientes": registros})
    finally:
        cli.close()


def main():
    p = argparse.ArgumentParser(description="Control de conflictos del Taller 1")
    sub = p.add_subparsers(dest="accion", required=True)
    sub.add_parser("snapshot", help="Captura vector e inventario en este nodo; no escribe a MongoDB")
    pr = sub.add_parser("propose", help="Genera propuesta TENTATIVA local, sin escribir a MongoDB")
    pr.add_argument("--resource", required=True, help="Ej. nodo_3.cpu_cores")
    pr.add_argument("--value", required=True, type=int, help="Nuevo valor propuesto")
    co = sub.add_parser("compare", help="Compara dos archivos de propuestas offline")
    co.add_argument("--a", required=True)
    co.add_argument("--b", required=True)
    rec = sub.add_parser("reconcile", help="Confirma un conflicto usando lease y mayoria MongoDB")
    rec.add_argument("--a", required=True)
    rec.add_argument("--b", required=True)
    sub.add_parser("metrics", help="Consulta contadores y auditoria en MongoDB")
    args = p.parse_args()
    try:
        {"snapshot": snapshot, "propose": propose, "compare": analizar,
         "reconcile": reconciliar, "metrics": metricas}[args.accion](args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
