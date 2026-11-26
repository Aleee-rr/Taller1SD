import os
import time
import logging
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import ReturnDocument

from common.db import bd, asegurar_indices
from common.lamport import LamportClock
from common.lease import lease_vigente
from common.mq import Productor, nuevo_evento
from coordinador import worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger("api")

NODE_ID = int(os.environ["NODE_ID"])

d = bd()
reloj = LamportClock()
prod = Productor()

app = FastAPI(
    title="Coordinador de Recursos Distribuidos",
    description="API REST para la gestión de asignaciones y métricas",
    version="1.0.0"
)


class Metricas(BaseModel):
    node_id: int
    lamport: int = Field(ge=0)
    cpu_pct: float = Field(ge=0, le=100)
    mem_mb: float = Field(ge=0)


class Solicitud(BaseModel):
    node_id: int
    lamport: int = Field(ge=0)
    recurso: str = "memoria_mb"
    cantidad: int = Field(default=128, gt=0)
    falla_en_paso: Optional[int] = None
    lote: Optional[str] = None
    message_id: Optional[str] = None


def exigir_lider():
    lease = lease_vigente(d)

    if not lease:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "NOT_LEADER",
                "node_id": NODE_ID,
                "mensaje": "Este nodo no tiene el liderazgo vigente"
            }
        )

    return lease


@app.on_event("startup")
def iniciar():
    asegurar_indices(d)

    threading.Thread(
        target=worker.correr,
        args=(reloj,),
        daemon=True,
        name="worker-allocations"
    ).start()

    threading.Thread(
        target=worker.vigilar_nodos,
        args=(reloj, prod),
        daemon=True,
        name="watchdog-nodos"
    ).start()

    log.info("API iniciada en nodo %s", NODE_ID)


@app.get("/v1/health")
def health():
    lider = lease_vigente(d) is not None

    return {
        "ok": True,
        "node_id": NODE_ID,
        "lider": lider,
        "lamport": reloj.value
    }


@app.post("/v1/metrics")
def recibir_metricas(m: Metricas):
    exigir_lider()

    nuevo_lamport = reloj.observe(m.lamport)

    d.metricas.update_one(
        {"node_id": m.node_id},
        {
            "$set": {
                "node_id": m.node_id,
                "lamport": nuevo_lamport,
                "cpu_pct": m.cpu_pct,
                "mem_mb": m.mem_mb,
                "ts": time.time()
            }
        },
        upsert=True
    )

    asignacion = d.notificaciones.find_one_and_update(
        {
            "node_id": m.node_id,
            "entregada": False
        },
        {
            "$set": {
                "entregada": True,
                "ts_entrega": time.time()
            }
        },
        return_document=ReturnDocument.AFTER
    )

    return {
        "ack": True,
        "node_id": m.node_id,
        "lamport": nuevo_lamport,
        "assignment": asignacion
    }


@app.post("/v1/assignments", status_code=202)
def crear_asignacion(s: Solicitud):
    exigir_lider()

    reloj.observe(s.lamport)

    payload = {
        "recurso": s.recurso,
        "cantidad": s.cantidad
    }

    if s.falla_en_paso is not None:
        if s.falla_en_paso not in (1, 2, 3):
            raise HTTPException(
                status_code=400,
                detail="falla_en_paso debe ser 1, 2 o 3"
            )
        payload["falla_en_paso"] = s.falla_en_paso

    if s.lote is not None:
        payload["lote"] = s.lote

    evento = nuevo_evento(
        "allocation_request",
        s.node_id,
        s.lamport,
        payload,
        message_id=s.message_id
    )

    try:
        prod.publicar("allocations", evento, intentos=2)

    except RuntimeError as e:
        log.exception("No se pudo publicar la asignación")

        raise HTTPException(
            status_code=503,
            detail={
                "error": "BROKER_UNAVAILABLE",
                "mensaje": str(e)
            }
        )

    log.info(
        "Asignación publicada: request_id=%s nodo=%s",
        evento["message_id"],
        s.node_id
    )

    return JSONResponse(
        status_code=202,
        content={
            "accepted": True,
            "request_id": evento["message_id"],
            "lamport": reloj.value
        }
    )


@app.get("/v1/sagas/{saga_id}")
def consultar_saga(saga_id: str):
    saga = d.sagas.find_one(
        {"saga_id": saga_id},
        {"_id": 0}
    )

    if not saga:
        raise HTTPException(
            status_code=404,
            detail="Saga no encontrada"
        )

    return saga
