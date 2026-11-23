import os, json, time, uuid, datetime, logging, threading
import pika
from pika.exceptions import AMQPError

log = logging.getLogger('mq')
COLAS = ['metrics', 'allocations', 'failover']

def nuevo_evento(tipo, node_id, lamport, payload, message_id=None):
    return {'message_id': message_id or str(uuid.uuid4()), 'type': tipo, 'node_id': node_id,
            'lamport': lamport, 'ts': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'payload': payload}

def conectar(intentos=None):
    espera, n = 1, 0
    while True:
        try:
            cred = pika.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASS'])
            con = pika.BlockingConnection(pika.ConnectionParameters(host=os.environ['BROKER_HOST'], credentials=cred, heartbeat=30, connection_attempts=1, socket_timeout=5))
            ch = con.channel()
            ch.confirm_delivery()
            for q in COLAS:
                ch.queue_declare(queue=q, durable=True)
            return con, ch
        except (AMQPError, OSError) as e:
            n += 1
            if intentos is not None and n >= intentos:
                raise RuntimeError('broker no disponible') from e
            log.warning('broker no disponible (%s); reintento en %ss', e, espera)
            time.sleep(espera)
            espera = min(espera * 2, 30)

class Productor:
    def __init__(self):
        self.con = self.ch = None
        self.lock = threading.Lock()

    def publicar(self, cola, ev, intentos=None):
        cuerpo = json.dumps(ev).encode()
        with self.lock:
            n = 0
            while True:
                try:
                    if self.con is None or self.con.is_closed:
                        self.con, self.ch = conectar(intentos=1 if intentos else None)
                    self.ch.basic_publish('', cola, cuerpo, pika.BasicProperties(delivery_mode=2, content_type='application/json', message_id=ev['message_id']), mandatory=True)
                    return
                except (AMQPError, OSError, RuntimeError) as e:
                    self.con = None
                    n += 1
                    if intentos is not None and n >= intentos:
                        raise RuntimeError('no se pudo publicar') from e
                    log.warning('publicación fallida (%s); reintento', e)
                    time.sleep(min(2 ** n, 30))
