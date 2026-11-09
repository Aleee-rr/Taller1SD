import argparse
import json
import logging
import os
import socket
import socketserver
import sys
import threading
import time


INTERVALO_LATIDOS = 1.0
TIEMPO_LIMITE_LIDER = 3.5
TIEMPO_RESPUESTA = 1.0
ESPERA_COORDINADOR = 5.0

SEGUIDOR = "SEGUIDOR"
CANDIDATO = "CANDIDATO"
LIDER = "LIDER"


class Nodo:
    def __init__(self, id_nodo, nodos):
        self.id = id_nodo

        self.nodos = {i: direccion for i, direccion in nodos.items()
                      if i != id_nodo}

        self.bloqueo = threading.RLock()
        self.estado = SEGUIDOR
        self.termino = 0
        self.lider = None
        self.reloj = 0
        self.ultimo_latido = time.monotonic()
        self.en_eleccion = False
        self.asignaciones = []
        self.nodos_activos = {}

        os.makedirs("logs", exist_ok=True)
        self.logger = logging.getLogger(f"nodo{id_nodo}")
        self.logger.setLevel(logging.INFO)
        formato = logging.Formatter("%(asctime)s.%(msecs)03d %(message)s",
                                    "%H:%M:%S")
        for manejador in (logging.StreamHandler(sys.stdout),
                          logging.FileHandler(f"logs/nodo{id_nodo}.log")):
            manejador.setFormatter(formato)
            self.logger.addHandler(manejador)


    def registrar(self, mensaje):
        self.logger.info("[nodo %d | %s | term=%d | L=%d] %s",
                         self.id, self.estado, self.termino, self.reloj,
                         mensaje)

    def avanzar_reloj(self, reloj_recibido=0):
        with self.bloqueo:
            self.reloj = max(self.reloj, reloj_recibido) + 1
            return self.reloj

    def responder(self, tipo, **datos):
        return {"type": tipo, "from": self.id, "term": self.termino,
                "lamport": self.avanzar_reloj(), **datos}

    def adoptar_termino(self, termino_recibido, motivo):
        with self.bloqueo:
            if termino_recibido > self.termino:
                era_lider = self.estado == LIDER
                self.termino = termino_recibido
                if era_lider:
                    self.estado = SEGUIDOR
                    self.lider = None
                    self.registrar(
                        f"DEJO DE SER LÍDER: vi un término mayor ({motivo})")

    def marcar_nodo(self, id_nodo, activo):
        with self.bloqueo:
            anterior = self.nodos_activos.get(id_nodo)
            self.nodos_activos[id_nodo] = activo
        if anterior != activo:
            self.registrar(f"nodo {id_nodo} "
                           f"{'RESPONDE' if activo else 'NO RESPONDE'}")

    def enviar(self, id_nodo, tipo, **datos):
        host, puerto = self.nodos[id_nodo]
        mensaje = {"type": tipo, "from": self.id, "term": self.termino,
                   "lamport": self.avanzar_reloj(), **datos}
        try:
            with socket.create_connection((host, puerto),
                                          timeout=TIEMPO_RESPUESTA) as conexion:
                conexion.settimeout(TIEMPO_RESPUESTA)
                conexion.sendall((json.dumps(mensaje) + "\n").encode())
                respuesta = json.loads(conexion.makefile("r").readline())
        except (OSError, ValueError):
            self.marcar_nodo(id_nodo, False)
            return None
        self.marcar_nodo(id_nodo, True)
        self.avanzar_reloj(respuesta.get("lamport", 0))
        self.adoptar_termino(respuesta.get("term", 0),
                             f"respuesta del nodo {id_nodo}")
        return respuesta


    def manejar_mensaje(self, mensaje):
        self.avanzar_reloj(mensaje.get("lamport", 0))
        tipo = mensaje.get("type")
        emisor = mensaje.get("from")
        termino_recibido = mensaje.get("term", 0)

        if tipo == "ELECTION":
            self.registrar(f"ELECTION de {emisor}; respondo OK")
            self.nodo_menor_detectado(emisor)
            return self.responder("OK")
        if tipo == "COORDINATOR":
            return self.al_recibir_coordinador(emisor, termino_recibido)
        if tipo == "HEARTBEAT":
            return self.al_recibir_latido(emisor, termino_recibido)
        if tipo == "ASSIGN":
            return self.al_recibir_asignacion(mensaje)
        if tipo == "STATUS":
            return self.responder("STATUS", state=self.estado,
                                  leader=self.lider,
                                  assignments=self.asignaciones)
        return self.responder("ERROR", reason="tipo desconocido")

    def nodo_menor_detectado(self, emisor):
        if self.estado == LIDER:
            threading.Thread(target=self.enviar,
                             args=(emisor, "COORDINATOR"),
                             daemon=True).start()
        else:
            threading.Thread(target=self.iniciar_eleccion,
                             daemon=True).start()

    def al_recibir_coordinador(self, emisor, termino_recibido):
        with self.bloqueo:
            self.adoptar_termino(termino_recibido,
                                 f"COORDINATOR de {emisor}")


            if termino_recibido < self.termino:
                self.registrar(f"RECHAZO COORDINATOR de {emisor}: término "
                               f"obsoleto ({termino_recibido} < "
                               f"{self.termino})")
                return self.responder("REJECT", reason="stale_term")


            if emisor < self.id:
                self.registrar(f"RECHAZO COORDINATOR de {emisor}: "
                               f"mi id es mayor")
                self.nodo_menor_detectado(emisor)
                return self.responder("REJECT", reason="lower_id")

            self.estado = SEGUIDOR
            self.lider = emisor
            self.ultimo_latido = time.monotonic()
            self.registrar(f"ACEPTO a {emisor} como coordinador "
                           f"(término {termino_recibido})")
            return self.responder("ACCEPT")

    def al_recibir_latido(self, emisor, termino_recibido):
        with self.bloqueo:
            self.adoptar_termino(termino_recibido, f"HEARTBEAT de {emisor}")

            if termino_recibido < self.termino:
                self.registrar(f"HEARTBEAT obsoleto de {emisor} "
                               f"({termino_recibido} < {self.termino})")
                return self.responder("STALE", leader=self.lider)

            if emisor < self.id:
                self.nodo_menor_detectado(emisor)
                return self.responder("REJECT", reason="lower_id")

            self.estado = SEGUIDOR
            self.lider = emisor
            self.ultimo_latido = time.monotonic()
            return self.responder("OK")

    def al_recibir_asignacion(self, mensaje):
        with self.bloqueo:
            if self.estado != LIDER:
                self.registrar(f"RECHAZO ASSIGN: no soy líder "
                               f"(líder: {self.lider})")
                return self.responder("NOT_LEADER", leader=self.lider)

            asignacion = {"resource": mensaje.get("resource"),
                          "node": self.id,
                          "term": self.termino,
                          "lamport": self.avanzar_reloj()}
            self.asignaciones.append(asignacion)
            self.registrar(f"ASIGNACIÓN {asignacion}")
            return self.responder("ASSIGNED", entry=asignacion)


    def iniciar_eleccion(self):
        with self.bloqueo:
            if self.en_eleccion:
                return
            self.en_eleccion = True
            self.estado = CANDIDATO
            tiempo_inicio = time.monotonic()

        try:
            self.registrar("INICIO ELECCIÓN")
            recibio_ok = False


            for id_nodo in sorted(i for i in self.nodos if i > self.id):
                respuesta = self.enviar(id_nodo, "ELECTION")
                if respuesta and respuesta.get("type") == "OK":
                    recibio_ok = True
                    self.registrar(f"nodo {id_nodo} (id mayor) respondió OK")


            if not recibio_ok:
                self.convertirse_en_lider()
                return


            limite = time.monotonic() + ESPERA_COORDINADOR
            while time.monotonic() < limite:
                if self.ultimo_latido > tiempo_inicio:
                    return
                time.sleep(0.1)

            self.registrar("nadie anunció coordinador; reintentaré")
            with self.bloqueo:
                if self.estado == CANDIDATO:
                    self.estado = SEGUIDOR
        finally:
            with self.bloqueo:
                self.en_eleccion = False

    def convertirse_en_lider(self):
        with self.bloqueo:
            self.termino += 1
            self.estado = LIDER
            self.lider = self.id
            termino_actual = self.termino

        self.registrar(f"GANÉ LA ELECCIÓN: soy el coordinador "
                       f"(término {termino_actual})")

        for id_nodo in self.nodos:
            if self.estado != LIDER:
                break
            respuesta = self.enviar(id_nodo, "COORDINATOR")
            if respuesta:
                self.registrar(f"nodo {id_nodo} respondió "
                               f"{respuesta.get('type')} al COORDINATOR")


    def ciclo_latidos(self):
        while True:
            time.sleep(INTERVALO_LATIDOS)
            if self.estado == LIDER:
                for id_nodo in self.nodos:
                    threading.Thread(target=self.enviar,
                                     args=(id_nodo, "HEARTBEAT"),
                                     daemon=True).start()

    def ciclo_monitor(self):
        time.sleep(2)
        self.iniciar_eleccion()
        while True:
            time.sleep(0.2)
            with self.bloqueo:
                necesita_eleccion = (
                    self.estado != LIDER
                    and not self.en_eleccion
                    and time.monotonic() - self.ultimo_latido
                    > TIEMPO_LIMITE_LIDER)
            if necesita_eleccion:
                self.registrar("sin latido del líder: inicio elección")
                self.iniciar_eleccion()


class ManejadorTCP(socketserver.StreamRequestHandler):
    timeout = 5


    def handle(self):
        try:
            mensaje = json.loads(self.rfile.readline())
            respuesta = self.server.nodo.manejar_mensaje(mensaje)
        except Exception as error:
            respuesta = {"type": "ERROR", "reason": str(error)}
        self.wfile.write((json.dumps(respuesta) + "\n").encode())


class Servidor(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def leer_nodos(texto):
    nodos = {}
    for elemento in texto.split(","):
        id_nodo, direccion = elemento.split("=")
        host, puerto = direccion.rsplit(":", 1)
        nodos[int(id_nodo)] = (host, int(puerto))
    return nodos


def consultar(destino, mensaje):
    host, puerto = destino.rsplit(":", 1)
    mensaje = {"from": 0, "term": 0, "lamport": 0, **mensaje}
    with socket.create_connection((host, int(puerto)),
                                  timeout=3) as conexion:
        conexion.sendall((json.dumps(mensaje) + "\n").encode())
        return json.loads(conexion.makefile("r").readline())


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    comandos = parser.add_subparsers(dest="cmd", required=True)

    ejecutar = comandos.add_parser("run", help="arrancar un nodo")
    ejecutar.add_argument("--id", type=int, required=True)
    ejecutar.add_argument("--peers", required=True,
                          help="id=ip:puerto,... incluyendo este nodo")

    estado = comandos.add_parser("status",
                                 help="consultar el estado de un nodo")
    estado.add_argument("--target", required=True, help="ip:puerto")

    asignar = comandos.add_parser(
        "assign", help="pedir una asignación (solo el líder acepta)")
    asignar.add_argument("--target", required=True, help="ip:puerto")
    asignar.add_argument("--resource", required=True)

    argumentos = parser.parse_args()

    if argumentos.cmd == "status":
        print(json.dumps(consultar(argumentos.target, {"type": "STATUS"}),
                         indent=2))
    elif argumentos.cmd == "assign":
        print(json.dumps(
            consultar(argumentos.target,
                      {"type": "ASSIGN", "resource": argumentos.resource}),
            indent=2))
    else:
        nodos = leer_nodos(argumentos.peers)
        nodo = Nodo(argumentos.id, nodos)
        servidor = Servidor(("0.0.0.0", nodos[argumentos.id][1]),
                            ManejadorTCP)
        servidor.nodo = nodo
        nodo.registrar(f"escuchando en el puerto {nodos[argumentos.id][1]}")
        threading.Thread(target=nodo.ciclo_latidos, daemon=True).start()
        threading.Thread(target=nodo.ciclo_monitor, daemon=True).start()
        try:
            servidor.serve_forever()
        except KeyboardInterrupt:
            nodo.registrar("nodo detenido")


if __name__ == "__main__":
    main()
