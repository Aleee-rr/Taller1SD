"""Nodo Bully con una unica autoridad para asignaciones: lease MongoDB con fencing.

Bully decide que nodo intenta liderar. Solo se anuncia LIDER despues de adquirir
un lease escrito con w=majority. Cada ASSIGN valida el lease dentro de una
transaccion que escribe el documento del lease y el inventario atomicamente.
"""

import argparse
import json
import logging
import os
import socket
import socketserver
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

SEGUIDOR, CANDIDATO, LIDER = "SEGUIDOR", "CANDIDATO", "LIDER"
HEARTBEAT_INTERVAL = 2.0
LEADER_TIMEOUT = 7.0
RPC_TIMEOUT = 3.0
RETRY_ELECTION = 2.5
LEASE_SECONDS = 12


def utcnow():
    return datetime.now(timezone.utc)


class LeasePerdido(Exception):
    pass


class AutoridadMongo:
    """Token de fencing y lease confirmados por la mayoria de MongoDB."""

    def __init__(self, uri):
        self.client = MongoClient(
            uri, tz_aware=True, serverSelectionTimeoutMS=2500,
            connectTimeoutMS=2500, socketTimeoutMS=4000,
            appname="taller1sd-bully"
        )
        self.db = self.client.get_database(
            "recursos_borde",
            write_concern=WriteConcern("majority", wtimeout=4000),
            read_concern=ReadConcern("majority")
        )
        self.coordinacion = self.db["coordinacion"]
        self.inventario = self.db["inventario"]

    def inicializar(self):
        try:
            self.coordinacion.update_one(
                {"_id": "bully"},
                {"$setOnInsert": {
                    "owner": None, "token": None, "epoch": 0,
                    "lease_until": datetime(1970, 1, 1, tzinfo=timezone.utc)
                }},
                upsert=True
            )
        except DuplicateKeyError:
            pass  # Otro nodo inicializo el mismo _id al mismo tiempo.

    def consultar(self):
        return self.coordinacion.find_one({"_id": "bully"})

    def adquirir(self, id_nodo):
        self.inicializar()
        ahora = utcnow()
        return self.coordinacion.find_one_and_update(
            {"_id": "bully", "lease_until": {"$lte": ahora}},
            {
                "$set": {
                    "owner": id_nodo,
                    "token": uuid.uuid4().hex,
                    "lease_until": ahora + timedelta(seconds=LEASE_SECONDS),
                },
                "$inc": {"epoch": 1},
            },
            return_document=ReturnDocument.AFTER
        )

    def renovar(self, id_nodo, token, epoch):
        ahora = utcnow()
        return self.coordinacion.find_one_and_update(
            {
                "_id": "bully", "owner": id_nodo,
                "token": token, "epoch": epoch,
                "lease_until": {"$gt": ahora},
            },
            {"$set": {"lease_until": ahora + timedelta(seconds=LEASE_SECONDS)}},
            return_document=ReturnDocument.AFTER
        )

    def liberar(self, id_nodo, token, epoch):
        if token is None:
            return
        self.coordinacion.update_one(
            {"_id": "bully", "owner": id_nodo, "token": token, "epoch": epoch},
            {"$set": {"lease_until": utcnow()}}
        )

    def validar(self, id_nodo, token, epoch):
        if not token or not isinstance(epoch, int):
            return False
        return self.coordinacion.find_one({
            "_id": "bully", "owner": id_nodo,
            "token": token, "epoch": epoch,
            "lease_until": {"$gt": utcnow()},
        }) is not None

    def asignar(self, id_nodo, token, epoch, recurso, lamport):
        """Fencing atomico: tocar lease e inventario en la MISMA transaccion."""
        if not isinstance(recurso, str) or not recurso.strip():
            raise ValueError("Recurso vacio")
        evento = {
            "op_id": uuid.uuid4().hex,
            "resource": recurso.strip(), "node": id_nodo,
            "epoch": epoch, "lamport": lamport,
            "created_at": utcnow(),
        }

        def operacion(session):
            lease = self.coordinacion.find_one_and_update(
                {
                    "_id": "bully", "owner": id_nodo, "token": token,
                    "epoch": epoch, "lease_until": {"$gt": utcnow()},
                },
                {"$inc": {"operation_count": 1}},
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if lease is None:
                raise LeasePerdido("El lease ya no pertenece a este nodo")
            self.inventario.update_one(
                {"_id": "inventario_global"},
                {
                    "$inc": {f"vector_version.n{id_nodo}": 1},
                    "$push": {"historial_asignaciones": evento},
                },
                upsert=True, session=session,
            )
            return evento

        with self.client.start_session() as session:
            return session.with_transaction(
                operacion,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority", wtimeout=4000),
                read_preference=ReadPreference.PRIMARY,
                max_commit_time_ms=4000,
            )


class Nodo:
    def __init__(self, id_nodo, nodos, autoridad):
        if id_nodo not in nodos:
            raise ValueError("El ID local debe aparecer en --peers")
        self.id = id_nodo
        self.nodos = {i: addr for i, addr in nodos.items() if i != id_nodo}
        self.autoridad = autoridad
        self.bloqueo = threading.RLock()
        self.estado = SEGUIDOR
        self.lider = None
        self.termino = 0  # Epoch de MongoDB; no se inventa localmente.
        self.token = None
        self.ultimo_latido = 0.0
        self.en_eleccion = False
        self.proxima_eleccion = 0.0
        self.reloj = 0
        self.nodos_activos = {}
        self.asignaciones = []
        self.detener = threading.Event()
        os.makedirs("logs", exist_ok=True)
        self.logger = logging.getLogger(f"sdm-nodo-{id_nodo}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            formato = logging.Formatter("%(asctime)s %(message)s", "%H:%M:%S")
            for h in (logging.StreamHandler(sys.stdout),
                      logging.FileHandler(f"logs/nodo{id_nodo}.log")):
                h.setFormatter(formato)
                self.logger.addHandler(h)

    def registrar(self, texto):
        self.logger.info("[nodo %s | %s | epoch=%s] %s",
                         self.id, self.estado, self.termino, texto)

    def avanzar_reloj(self, remoto=0):
        with self.bloqueo:
            self.reloj = max(self.reloj, remoto if isinstance(remoto, int) else 0) + 1
            return self.reloj

    def responder(self, tipo, **datos):
        with self.bloqueo:
            return {"type": tipo, "from": self.id, "epoch": self.termino,
                    "lamport": self.avanzar_reloj(), **datos}

    def marcar_nodo(self, destino, responde):
        with self.bloqueo:
            anterior = self.nodos_activos.get(destino)
            self.nodos_activos[destino] = responde
        if anterior != responde:
            self.registrar(f"nodo {destino} {'RESPONDE' if responde else 'NO RESPONDE'}")

    def enviar(self, destino, tipo, **datos):
        host, puerto = self.nodos[destino]
        mensaje = self.responder(tipo, **datos)
        try:
            with socket.create_connection((host, puerto), timeout=RPC_TIMEOUT) as s:
                s.settimeout(RPC_TIMEOUT)
                s.sendall((json.dumps(mensaje) + "\n").encode())
                respuesta = json.loads(s.makefile("r").readline())
            if respuesta.get("from") != destino:
                raise ValueError("ID incorrecto en la respuesta")
        except (OSError, ValueError, json.JSONDecodeError):
            self.marcar_nodo(destino, False)
            return None
        self.marcar_nodo(destino, True)
        self.avanzar_reloj(respuesta.get("lamport", 0))
        if (tipo == "HEARTBEAT" and destino > self.id and
                respuesta.get("type") == "REJECT" and
                respuesta.get("reason") == "higher_id_alive"):
            self.dejar_liderazgo("reaparecio un nodo de mayor ID")
        return respuesta

    def dejar_liderazgo(self, motivo, liberar=True):
        with self.bloqueo:
            if self.estado != LIDER:
                return
            token, epoch = self.token, self.termino
            self.token = None
            self.estado = SEGUIDOR
            self.lider = None
            self.ultimo_latido = 0
            self.proxima_eleccion = time.monotonic() + RETRY_ELECTION
        self.registrar(f"DEJO DE SER LIDER: {motivo}")
        if liberar:
            try:
                self.autoridad.liberar(self.id, token, epoch)
            except PyMongoError as e:
                self.registrar(f"No se pudo liberar el lease; expirara: {e}")

    def manejar_mensaje(self, msg):
        self.avanzar_reloj(msg.get("lamport", 0))
        tipo = msg.get("type")
        emisor = msg.get("from")
        if tipo == "STATUS":
            with self.bloqueo:
                estado, lider, token, epoch = self.estado, self.lider, self.token, self.termino
                asignaciones = list(self.asignaciones)
            autorizado = False
            if estado == LIDER and token is not None:
                try:
                    autorizado = self.autoridad.validar(self.id, token, epoch)
                except PyMongoError:
                    pass
            return self.responder("STATUS", state=estado, leader=lider,
                                 assignments=asignaciones,
                                 can_assign=autorizado)
        if tipo == "ASSIGN":
            return self.al_recibir_asignacion(msg)
        if emisor not in self.nodos:
            return self.responder("REJECT", reason="unknown_peer")
        if tipo == "PING":
            return self.responder("PONG", challenge=msg.get("challenge"))
        if tipo == "ELECTION":
            if emisor >= self.id:
                return self.responder("REJECT", reason="not_higher")
            self.programar_eleccion()
            return self.responder("OK")
        if tipo == "PREEMPT":
            return self.al_recibir_preempt(emisor)
        if tipo in ("COORDINATOR", "HEARTBEAT"):
            return self.al_recibir_lider(msg)
        return self.responder("ERROR", reason="tipo desconocido")

    def programar_eleccion(self):
        with self.bloqueo:
            if self.estado == LIDER or self.en_eleccion:
                return
        threading.Thread(target=self.iniciar_eleccion, daemon=True).start()

    def al_recibir_preempt(self, emisor):
        if emisor <= self.id:
            return self.responder("REJECT", reason="lower_id")
        # Verificar en la IP Tailscale del nodo de ID mayor, no confiar en el JSON.
        desafio = uuid.uuid4().hex
        prueba = self.enviar(emisor, "PING", challenge=desafio)
        if not prueba or prueba.get("type") != "PONG" or prueba.get("challenge") != desafio:
            return self.responder("REJECT", reason="peer_not_verified")
        self.dejar_liderazgo(f"preemption verificada del nodo {emisor}")
        return self.responder("YIELD")

    def al_recibir_lider(self, msg):
        emisor = msg["from"]
        if emisor < self.id:
            self.programar_eleccion()
            return self.responder("REJECT", reason="higher_id_alive")
        token, epoch = msg.get("token"), msg.get("epoch")
        try:
            valido = self.autoridad.validar(emisor, token, epoch)
        except PyMongoError:
            return self.responder("REJECT", reason="mongo_unavailable")
        if not valido:
            return self.responder("REJECT", reason="invalid_lease")
        # Cualquier lider antiguo se retira, pero el DB aplica fencing real.
        with self.bloqueo:
            era_lider = self.estado == LIDER
        if era_lider:
            self.dejar_liderazgo("MongoDB reconoce otro lease", liberar=False)
        with self.bloqueo:
            nuevo = self.lider != emisor or self.termino != epoch
            self.estado = SEGUIDOR
            self.lider = emisor
            self.termino = epoch
            self.ultimo_latido = time.monotonic()
        if nuevo:
            self.registrar(f"ACEPTO coordinador {emisor} (epoch {epoch})")
        return self.responder("ACCEPT")

    def al_recibir_asignacion(self, msg):
        with self.bloqueo:
            if self.estado != LIDER or self.token is None:
                return self.responder("NOT_LEADER", leader=self.lider)
            token, epoch = self.token, self.termino
            lamport = self.avanzar_reloj()
        try:
            evento = self.autoridad.asignar(self.id, token, epoch,
                                            msg.get("resource"), lamport)
        except (LeasePerdido, PyMongoError, ValueError) as e:
            self.registrar(f"ASSIGN RECHAZADO: {e}")
            if isinstance(e, LeasePerdido):
                self.dejar_liderazgo("perdio el fencing", liberar=False)
            return self.responder("REJECTED", reason="lease_or_database_unavailable")
        with self.bloqueo:
            self.asignaciones.append(evento)
        self.registrar(f"ASSIGN CONFIRMADO: {evento['resource']} ({evento['op_id']})")
        return self.responder("ASSIGNED", entry=evento)

    def iniciar_eleccion(self):
        with self.bloqueo:
            if self.en_eleccion or self.estado == LIDER:
                return
            self.en_eleccion = True
            self.estado = CANDIDATO
            inicio = time.monotonic()
        try:
            self.registrar("INICIO ELECCION BULLY")
            superiores = sorted(i for i in self.nodos if i > self.id)
            alguno_vivo = False
            for i in superiores:
                respuesta = self.enviar(i, "ELECTION")
                if respuesta and respuesta.get("type") == "OK":
                    alguno_vivo = True
            if alguno_vivo:
                self.registrar("Hay nodo de mayor ID; espero anuncio valido")
                return
            with self.bloqueo:
                if self.estado != CANDIDATO or self.ultimo_latido > inicio:
                    return
            try:
                lease = self.autoridad.adquirir(self.id)
            except PyMongoError as e:
                self.registrar(f"Sin quorum MongoDB: no puedo ser lider ({e})")
                return
            if lease is None:
                try:
                    actual = self.autoridad.consultar()
                except PyMongoError:
                    actual = None
                if actual and isinstance(actual.get("owner"), int) and actual["owner"] < self.id:
                    i = actual["owner"]
                    if i in self.nodos:
                        self.registrar(f"Solicitando cesion al nodo {i} de ID menor")
                        self.enviar(i, "PREEMPT")
                return
            with self.bloqueo:
                if self.estado != CANDIDATO or self.ultimo_latido > inicio:
                    try:
                        self.autoridad.liberar(self.id, lease["token"], lease["epoch"])
                    except PyMongoError:
                        pass
                    return
                self.estado = LIDER
                self.lider = self.id
                self.token = lease["token"]
                self.termino = lease["epoch"]
            self.registrar("GANÉ LA ELECCIÓN: lease w=majority confirmado")
            self.anunciar_lider()
        finally:
            with self.bloqueo:
                if self.estado == CANDIDATO:
                    self.estado = SEGUIDOR
                self.en_eleccion = False
                self.proxima_eleccion = time.monotonic() + RETRY_ELECTION

    def anunciar_lider(self):
        with self.bloqueo:
            if self.estado != LIDER:
                return
            token, epoch = self.token, self.termino
        for destino in sorted(self.nodos):
            self.enviar(destino, "COORDINATOR", token=token, epoch=epoch)

    def ciclo_latidos(self):
        while not self.detener.wait(HEARTBEAT_INTERVAL):
            with self.bloqueo:
                if self.estado != LIDER:
                    continue
                token, epoch = self.token, self.termino
            try:
                actualizado = self.autoridad.renovar(self.id, token, epoch)
            except PyMongoError as e:
                self.dejar_liderazgo(f"no hay quorum para renovar lease: {e}", liberar=False)
                continue
            if actualizado is None:
                self.dejar_liderazgo("lease vencido/reemplazado", liberar=False)
                continue
            with self.bloqueo:
                if self.estado != LIDER or self.token != token:
                    continue
            for destino in sorted(self.nodos):
                threading.Thread(target=self.enviar,
                                 args=(destino, "HEARTBEAT"),
                                 kwargs={"token": token, "epoch": epoch},
                                 daemon=True).start()

    def ciclo_monitor(self):
        if self.detener.wait(1.0):
            return
        while not self.detener.wait(0.4):
            with self.bloqueo:
                debe = (self.estado != LIDER and not self.en_eleccion and
                        time.monotonic() >= self.proxima_eleccion and
                        time.monotonic() - self.ultimo_latido > LEADER_TIMEOUT)
            if debe:
                self.iniciar_eleccion()


class ManejadorTCP(socketserver.StreamRequestHandler):
    timeout = 6

    def handle(self):
        try:
            mensaje = json.loads(self.rfile.readline())
            respuesta = self.server.nodo.manejar_mensaje(mensaje)
        except Exception as e:
            respuesta = {"type": "ERROR", "reason": str(e)}
        self.wfile.write((json.dumps(respuesta, default=str) + "\n").encode())


class Servidor(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def leer_nodos(texto):
    nodos = {}
    for elemento in texto.split(","):
        clave, direccion = elemento.split("=", 1)
        host, puerto = direccion.rsplit(":", 1)
        nodos[int(clave)] = (host, int(puerto))
    if len(nodos) != 3:
        raise ValueError("El taller requiere exactamente tres nodos")
    return nodos


def consultar(destino, mensaje):
    host, puerto = destino.rsplit(":", 1)
    msg = {"from": 0, "epoch": 0, "lamport": 0, **mensaje}
    with socket.create_connection((host, int(puerto)), timeout=RPC_TIMEOUT) as s:
        s.settimeout(RPC_TIMEOUT)
        s.sendall((json.dumps(msg) + "\n").encode())
        return json.loads(s.makefile("r").readline())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    cmd = p.add_subparsers(dest="cmd", required=True)
    run = cmd.add_parser("run")
    run.add_argument("--id", type=int, required=True)
    run.add_argument("--peers", required=True)
    status = cmd.add_parser("status")
    status.add_argument("--target", required=True)
    assign = cmd.add_parser("assign")
    assign.add_argument("--target", required=True)
    assign.add_argument("--resource", required=True)
    args = p.parse_args()
    if args.cmd in ("status", "assign"):
        payload = ({"type": "STATUS"} if args.cmd == "status" else
                   {"type": "ASSIGN", "resource": args.resource})
        print(json.dumps(consultar(args.target, payload), indent=2, default=str))
        return
    nodos = leer_nodos(args.peers)
    autoridad = AutoridadMongo(os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/?replicaSet=rs-inventario"))
    nodo = Nodo(args.id, nodos, autoridad)
    servidor = Servidor(("0.0.0.0", nodos[args.id][1]), ManejadorTCP)
    servidor.nodo = nodo
    nodo.registrar(f"escuchando puerto {nodos[args.id][1]}")
    threading.Thread(target=nodo.ciclo_latidos, daemon=True).start()
    threading.Thread(target=nodo.ciclo_monitor, daemon=True).start()
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        nodo.registrar("nodo detenido; el lease expirara si era lider")
    finally:
        nodo.detener.set()
        servidor.server_close()


if __name__ == "__main__":
    main()
