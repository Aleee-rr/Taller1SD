"""Pruebas reproducibles del estado Bully sin modificar el MongoDB real.

Ejecucion: .venv/bin/python -m unittest discover -s tests -v
"""
import os
import sys
import threading
import time
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from nodo import Nodo, Servidor, ManejadorTCP, LEASE_SECONDS, LeasePerdido, utcnow
import nodo as modulo


class AutoridadSimulada:
    """No es MongoDB; solo permite probar transiciones y prioridades."""

    def __init__(self):
        self.lock = threading.RLock()
        self.owner = None
        self.token = None
        self.epoch = 0
        self.until = utcnow() - timedelta(seconds=1)
        self.ttl = 1.7
        self.disponible = True
        self.writes = []
        self.count = 0

    def adquirir(self, i):
        with self.lock:
            if not self.disponible or self.until > utcnow():
                return None
            self.epoch += 1
            self.owner = i
            self.count += 1
            self.token = f"token_{i}_{self.count}"
            self.until = utcnow() + timedelta(seconds=self.ttl)
            return {"owner": i, "token": self.token, "epoch": self.epoch}

    def renovar(self, i, token, epoch):
        with self.lock:
            if not self.disponible or not self.validar(i, token, epoch):
                return None
            self.until = utcnow() + timedelta(seconds=self.ttl)
            return {"owner": i, "token": token, "epoch": epoch}

    def liberar(self, i, token, epoch):
        with self.lock:
            if (self.owner, self.token, self.epoch) == (i, token, epoch):
                self.until = utcnow()

    def consultar(self):
        with self.lock:
            return {"owner": self.owner, "token": self.token,
                    "epoch": self.epoch, "lease_until": self.until}

    def validar(self, i, token, epoch):
        with self.lock:
            return (self.disponible and
                    (self.owner, self.token, self.epoch) == (i, token, epoch)
                    and self.until > utcnow())

    def asignar(self, i, token, epoch, resource, lamport):
        with self.lock:
            if not self.validar(i, token, epoch):
                raise LeasePerdido("fencing")
            self.writes.append((i, epoch, resource))
            return {"resource": resource, "epoch": epoch, "node": i}


class TestBully(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        modulo.HEARTBEAT_INTERVAL = 0.3
        modulo.LEADER_TIMEOUT = 0.95
        modulo.RPC_TIMEOUT = 0.8
        modulo.RETRY_ELECTION = 0.3

    def setUp(self):
        self.autoridad = AutoridadSimulada()
        # Puertos efimeros. Cada server usa 0 para asignar puerto, pero se
        # rellena el mapeo definitivo antes de iniciar los ciclos.
        self.nodos = {}
        self.servidores = {}
        self.threads = {}
        for i in (1, 2, 3):
            nodo = Nodo(i, {i: ("127.0.0.1", 0)}, self.autoridad)
            srv = Servidor(("127.0.0.1", 0), ManejadorTCP)
            srv.nodo = nodo
            self.nodos[i] = nodo
            self.servidores[i] = srv
        peers = {i: ("127.0.0.1", self.servidores[i].server_address[1]) for i in (1,2,3)}
        for i,nodo in self.nodos.items():
            nodo.nodos = {j:val for j,val in peers.items() if j != i}

    def start(self, i, nuevo=False):
        if nuevo:
            self.nodos[i] = Nodo(i, {i: ("127.0.0.1", 0)}, self.autoridad)
            old = self.servidores[i]
            srv = Servidor(("127.0.0.1", old.server_address[1]), ManejadorTCP)
            self.servidores[i] = srv
            srv.nodo = self.nodos[i]
        n=self.nodos[i];s=self.servidores[i]
        peers = {k: ("127.0.0.1",self.servidores[k].server_address[1]) for k in (1,2,3)}
        n.nodos = {k:v for k,v in peers.items() if k != i}
        self.threads[i]=[threading.Thread(target=fn,daemon=True) for fn in (s.serve_forever,n.ciclo_monitor,n.ciclo_latidos)]
        for t in self.threads[i]: t.start()

    def stop(self, i):
        n=self.nodos[i];n.detener.set()
        srv=self.servidores[i];srv.shutdown();srv.server_close()

    def tearDown(self):
        for i,nodo in self.nodos.items():
            nodo.detener.set()
        for i,srv in self.servidores.items():
            try:
                if i in self.threads and srv.fileno() >=0:
                    srv.shutdown()
                srv.server_close()
            except Exception:
                pass

    def hasta(self, condicion, segundos=10):
        limite=time.monotonic()+segundos
        while time.monotonic()<limite:
            if condicion():return True
            time.sleep(.08)
        return False

    def test_eleccion_caida_y_regreso(self):
        self.start(3)
        self.assertTrue(self.hasta(lambda: self.nodos[3].estado==modulo.LIDER))
        self.start(2)
        self.start(1)
        self.assertTrue(self.hasta(lambda: self.nodos[1].lider==3 and self.nodos[2].lider==3))
        # No acepta un coordinador con epoch/token falso.
        invalido=self.nodos[1].manejar_mensaje({"type":"COORDINATOR", "from":2,
                                                 "epoch":10000,"token":"fake"})
        self.assertEqual(invalido["type"],"REJECT")
        self.stop(3)
        self.assertTrue(self.hasta(lambda: self.nodos[2].estado==modulo.LIDER and
                     self.nodos[1].lider==2, segundos=12))
        self.start(3, nuevo=True)
        self.assertTrue(self.hasta(lambda: self.nodos[3].estado==modulo.LIDER and
                     self.nodos[1].lider==3 and self.nodos[2].lider==3, segundos=12))
        self.assertTrue(self.autoridad.validar(3,self.nodos[3].token,self.nodos[3].termino))

    def test_lease_caido_no_autoriza_asignacion(self):
        self.start(3)
        self.assertTrue(self.hasta(lambda: self.nodos[3].estado==modulo.LIDER))
        with self.autoridad.lock:
            self.autoridad.disponible=False
        respuesta=self.nodos[3].manejar_mensaje({"type":"ASSIGN", "from":0,
                                                 "resource":"CPU", "lamport":0})
        self.assertEqual(respuesta["type"],"REJECTED")
        self.assertEqual(self.autoridad.writes,[])


if __name__ == "__main__":
    unittest.main()
