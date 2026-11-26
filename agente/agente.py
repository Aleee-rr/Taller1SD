import argparse
import concurrent.futures
import json
import logging
import os
import random
import time
import uuid

import psutil
import requests

from common.lamport import LamportClock
from common.mq import Productor, nuevo_evento


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger("agente")


def coordinadores():
    urls = os.environ.get("COORD_URLS", "")
    resultado = [u.strip().rstrip("/") for u in urls.split(",") if u.strip()]

    if not resultado:
        raise RuntimeError("No hay coordinadores configurados en COORD_URLS")

    return resultado


def esperar_reintento(numero):
    espera = min(2 ** numero, 16)
    log.warning("sin coordinador disponible; reintento en %ss", espera)
    time.sleep(espera)


def enviar_metricas(reloj, productor, urls, node_id):
    cpu = psutil.cpu_percent(interval=None)
    memoria = psutil.virtual_memory().used / (1024 * 1024)

    for intento in range(8):
        for url in urls:
            lamport = reloj.tick()

            datos = {
                "node_id": node_id,
                "lamport": lamport,
                "cpu_pct": cpu,
                "mem_mb": round(memoria, 2)
            }

            try:
                respuesta = requests.post(
                    f"{url}/v1/metrics",
                    json=datos,
                    timeout=(2, 3)
                )

                if respuesta.status_code == 409:
                    log.info("%s no es líder; pruebo el siguiente", url)
                    continue

                respuesta.raise_for_status()
                resultado = respuesta.json()

                if "lamport" in resultado:
                    reloj.observe(resultado["lamport"])

                evento = nuevo_evento(
                    "metrics",
                    node_id,
                    reloj.tick(),
                    {
                        "cpu_pct": cpu,
                        "mem_mb": round(memoria, 2)
                    }
                )

                productor.publicar("metrics", evento, intentos=2)

                log.info(
                    "métricas OK (L=%s) CPU=%.1f%% RAM=%.2f MB",
                    reloj.value,
                    cpu,
                    memoria
                )

                asignacion = resultado.get("assignment")

                if asignacion:
                    log.info(
                        "asignación recibida: %s",
                        json.dumps(asignacion, ensure_ascii=False, default=str)
                    )

                return True

            except requests.RequestException as e:
                log.warning("error con %s: %s; pruebo el siguiente", url, e)

            except (RuntimeError, ValueError) as e:
                log.warning("no se pudo completar el reporte: %s", e)

        if intento < 7:
            esperar_reintento(intento)

    log.error("no fue posible reportar métricas después de 8 intentos")
    return False


def demonio():
    node_id = int(os.environ["NODE_ID"])
    periodo = int(os.environ.get("PERIODO_S", "5"))

    urls = coordinadores()
    reloj = LamportClock()
    productor = Productor()

    log.info(
        "agente iniciado en nodo %s; periodo=%ss; coordinadores=%s",
        node_id,
        periodo,
        urls
    )

    while True:
        inicio = time.monotonic()

        enviar_metricas(reloj, productor, urls, node_id)

        transcurrido = time.monotonic() - inicio
        time.sleep(max(0, periodo - transcurrido))


def enviar_solicitud(args, reloj, urls, node_id, indice):
    lamport = reloj.tick()

    datos = {
        "node_id": node_id,
        "lamport": lamport,
        "recurso": args.recurso,
        "cantidad": args.cantidad
    }

    if args.falla_paso is not None:
        datos["falla_en_paso"] = args.falla_paso

    if args.lote:
        datos["lote"] = args.lote

    if args.message_id:
        datos["message_id"] = (
            args.message_id if args.rafaga == 1
            else f"{args.message_id}-{indice}"
        )

    for intento in range(8):
        for url in urls:
            try:
                respuesta = requests.post(
                    f"{url}/v1/assignments",
                    json=datos,
                    timeout=(2, 3)
                )

                if respuesta.status_code == 409:
                    log.info("%s no es líder; pruebo el siguiente", url)
                    continue

                respuesta.raise_for_status()
                resultado = respuesta.json()

                if "lamport" in resultado:
                    reloj.observe(resultado["lamport"])

                log.info(
                    "solicitud aceptada: %s",
                    json.dumps(resultado, ensure_ascii=False)
                )

                return resultado

            except requests.RequestException as e:
                log.warning("error con %s: %s; pruebo el siguiente", url, e)

        if intento < 7:
            esperar_reintento(intento)

    log.error("no se pudo enviar la solicitud número %s", indice)
    return None


def solicitar(args):
    node_id = int(os.environ["NODE_ID"])
    urls = coordinadores()

    reloj = LamportClock(start=args.lamport_inicial)

    solicitudes = list(range(1, args.rafaga + 1))

    if args.desordenar:
        random.shuffle(solicitudes)

    log.info(
        "enviando %s solicitud(es) desde el nodo %s",
        args.rafaga,
        node_id
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(args.rafaga, 10)
    ) as executor:
        tareas = [
            executor.submit(
                enviar_solicitud,
                args,
                reloj,
                urls,
                node_id,
                indice
            )
            for indice in solicitudes
        ]

        for tarea in concurrent.futures.as_completed(tareas):
            tarea.result()


def main():
    parser = argparse.ArgumentParser(
        description="Agente de métricas y solicitudes de recursos"
    )

    subparsers = parser.add_subparsers(dest="modo", required=True)

    subparsers.add_parser(
        "demonio",
        help="Reportar métricas periódicamente"
    )

    solicitud = subparsers.add_parser(
        "solicitar",
        help="Solicitar asignaciones al coordinador"
    )

    solicitud.add_argument("--recurso", default="memoria_mb")
    solicitud.add_argument("--cantidad", type=int, default=128)
    solicitud.add_argument("--falla-paso", type=int)
    solicitud.add_argument("--lote")
    solicitud.add_argument("--message-id")
    solicitud.add_argument("--rafaga", type=int, default=1)
    solicitud.add_argument("--desordenar", action="store_true")
    solicitud.add_argument("--lamport-inicial", type=int, default=0)

    args = parser.parse_args()

    if args.modo == "demonio":
        demonio()
    elif args.modo == "solicitar":
        if args.rafaga < 1:
            parser.error("--rafaga debe ser mayor que cero")
        if args.lamport_inicial < 0:
            parser.error("--lamport-inicial no puede ser negativo")
        solicitar(args)


if __name__ == "__main__":
    main()
