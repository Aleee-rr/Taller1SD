import os
import json
import time
import logging

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from common.db import bd
from common.lease import lease_vigente
from common.mq import conectar, nuevo_evento
from coordinador.saga import ejecutar_saga

log = logging.getLogger("worker")

VENTANA = float(os.getenv("VENTANA_S", "3"))

COLAS = {
    "allocations": "allocations",
    "metrics": "metrics",
    "failover": "failover",
}

NODE_ID = int(os.environ["NODE_ID"])
d = bd()


def procesar(ev, cola, reloj):
    """
    Procesa un evento de RabbitMQ de forma idempotente.
    """

    message_id = ev["message_id"]

    try:
        d.mensajes_procesados.insert_one({
            "message_id": message_id,
            "estado": "EN_PROCESO",
            "cola": cola,
            "node_id": NODE_ID,
            "ts_inicio": time.time()
        })

    except DuplicateKeyError:
        anterior = d.mensajes_procesados.find_one(
            {"message_id": message_id}
        )

        if anterior and anterior.get("estado") == "OK":
            log.info(
                "MENSAJE_DUPLICADO descartado: %s",
                message_id
            )
            return

    reloj.observe(ev.get("lamport", 0))

    contador = d.contadores.find_one_and_update(
        {"_id": "eventos_log"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )

    d.eventos_log.insert_one({
        "seq": contador["seq"],
        "message_id": message_id,
        "type": ev.get("type"),
        "node_id": ev.get("node_id"),
        "lamport": ev.get("lamport"),
        "cola": cola,
        "payload": ev.get("payload", {}),
        "ts_procesado": time.time()
    })

    log.info(
        "PROCESADO cola=%s message_id=%s lamport=%s nodo=%s",
        cola,
        message_id,
        ev.get("lamport"),
        ev.get("node_id")
    )

    if ev.get("type") == "allocation_request":
        ejecutar_saga(d, ev, reloj)

    d.mensajes_procesados.update_one(
        {"message_id": message_id},
        {
            "$set": {
                "estado": "OK",
                "ts_fin": time.time()
            }
        }
    )


def correr(reloj):
    """
    Worker consumidor.
    Solo consume mientras el nodo sea líder.
    Consume allocations y metrics.
    """

    while True:
        conexion = None
        canal = None
        buffer = []
        ultimo_mensaje = time.time()
        tags = {}

        try:
            conexion, canal = conectar()
            canal.basic_qos(prefetch_count=200)

            def recibir(ch, method, properties, body, cola):
                nonlocal ultimo_mensaje

                try:
                    ev = json.loads(body.decode())

                    if not buffer:
                        ultimo_mensaje = time.time()

                    buffer.append((
                        method.delivery_tag,
                        ev,
                        cola
                    ))

                except Exception:
                    log.exception(
                        "Mensaje inválido en cola %s",
                        cola
                    )

                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False
                    )

            while conexion.is_open:

                if lease_vigente(d):

                    if not tags:
                        for cola in ("allocations", "metrics", "failover"):

                            tags[cola] = canal.basic_consume(
                                queue=cola,
                                on_message_callback=lambda ch,
                                    method,
                                    properties,
                                    body,
                                    q=cola: recibir(
                                        ch,
                                        method,
                                        properties,
                                        body,
                                        q
                                    ),
                                auto_ack=False
                            )

                        log.info(
                            "WORKER activo como líder nodo=%s; "
                            "colas=%s",
                            NODE_ID,
                            list(tags.keys())
                        )

                else:

                    if tags:
                        for tag in tags.values():
                            canal.basic_cancel(tag)

                        tags.clear()

                        for delivery_tag, ev, cola in buffer:
                            canal.basic_nack(
                                delivery_tag=delivery_tag,
                                requeue=True
                            )

                        buffer.clear()

                        log.info("WORKER perdió liderazgo")

                if tags:

                    conexion.process_data_events(
                        time_limit=0.5
                    )

                    if buffer and (
                        time.time() - ultimo_mensaje >= VENTANA
                        or len(buffer) >= 200
                    ):

                        buffer.sort(
                            key=lambda item: (
                                int(item[1].get("lamport", 0)),
                                int(item[1].get("node_id", 0))
                            )
                        )

                        lote = buffer[:]
                        buffer.clear()

                        for delivery_tag, ev, cola in lote:

                            try:
                                if not lease_vigente(d):
                                    canal.basic_nack(
                                        delivery_tag=delivery_tag,
                                        requeue=True
                                    )
                                    continue

                                procesar(ev, cola, reloj)

                                canal.basic_ack(
                                    delivery_tag=delivery_tag
                                )

                            except Exception:
                                log.exception(
                                    "Error procesando mensaje %s",
                                    ev.get("message_id")
                                )

                                canal.basic_nack(
                                    delivery_tag=delivery_tag,
                                    requeue=True
                                )

                else:
                    conexion.process_data_events(
                        time_limit=1
                    )

        except Exception:
            log.exception(
                "Worker desconectado; intentando reconectar"
            )

        finally:
            if conexion is not None and conexion.is_open:
                try:
                    conexion.close()
                except Exception:
                    pass

        time.sleep(2)


def vigilar_nodos(reloj, productor):
    """
    Detecta nodos que dejan de enviar métricas.
    Solo el líder publica eventos node_down.
    """

    reportados = set()

    while True:
        try:
            if lease_vigente(d):
                ahora = time.time()

                for metrica in d.metricas.find():
                    nodo = int(metrica["node_id"])
                    ultima = float(metrica.get("ts", 0))

                    if nodo == NODE_ID:
                        continue

                    if ahora - ultima > 15 and nodo not in reportados:

                        evento = nuevo_evento(
                            "node_down",
                            NODE_ID,
                            reloj.tick(),
                            {
                                "node_id": nodo,
                                "ultima_metrica": ultima
                            }
                        )

                        productor.publicar(
                            "failover",
                            evento,
                            intentos=2
                        )

                        reportados.add(nodo)

                        log.warning(
                            "NODE_DOWN detectado nodo=%s",
                            nodo
                        )

                    elif ahora - ultima <= 15:
                        reportados.discard(nodo)

        except Exception:
            log.exception("Error vigilando nodos")

        time.sleep(5)
