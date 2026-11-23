import time, logging
from common.lease import lease_vigente, LEASE_ID

log = logging.getLogger('saga')

def _paso(d, sid, nombre, estado):
    d.sagas.update_one({'saga_id': sid}, {'$push': {'pasos': {'paso': nombre, 'estado': estado, 't': time.time()}}})

def _falla(p, n):
    if p.get('falla_en_paso') == n:
        raise RuntimeError(f'falla simulada en el paso {n}')

def reservar(d, sid, nodo, p):
    inv = d.inventario.find_one({'_id': 'inventario_global'})
    total = inv['estado_recursos'][f'nodo_{nodo}'][p['recurso']]
    activas = d.reservas.find({'nodo': nodo, 'recurso': p['recurso'], 'estado': {'$in': ['RESERVADA', 'CONFIRMADA']}})
    if sum(r['cantidad'] for r in activas) + p['cantidad'] > total:
        raise RuntimeError('recurso insuficiente')
    d.reservas.update_one({'saga_id': sid}, {'$set': {'nodo': nodo, 'recurso': p['recurso'], 'cantidad': p['cantidad'], 'estado': 'RESERVADA'}}, upsert=True)

def liberar(d, sid):
    d.reservas.update_one({'saga_id': sid}, {'$set': {'estado': 'LIBERADA'}})

def replicar(d, sid, nodo, p, lease):
    with d.client.start_session() as s:
        with s.start_transaction():
            actual = d.coordinacion.find_one({'_id': LEASE_ID}, session=s)
            if not actual or actual.get('token') != lease.get('token'):
                raise RuntimeError('fencing: el lease cambió')
            if d.inventario.find_one({'historial_asignaciones.saga_id': sid}, session=s):
                return
            d.inventario.update_one({'_id': 'inventario_global'},
                {'$push': {'historial_asignaciones': {'saga_id': sid, 'nodo': nodo, 'recurso': p['recurso'], 'cantidad': p['cantidad'], 'estado': 'ACTIVA', 'ts': time.time()}},
                 '$inc': {f'vector_version.n{nodo}': 1}}, session=s)

def anular_replica(d, sid, nodo):
    d.inventario.update_one({'_id': 'inventario_global', 'historial_asignaciones.saga_id': sid},
        {'$set': {'historial_asignaciones.$.estado': 'ANULADA'}, '$inc': {f'vector_version.n{nodo}': 1}})

def confirmar(d, sid, nodo, p):
    d.reservas.update_one({'saga_id': sid}, {'$set': {'estado': 'CONFIRMADA'}})
    d.notificaciones.update_one({'saga_id': sid}, {'$set': {'node_id': nodo, 'recurso': p['recurso'], 'cantidad': p['cantidad'], 'entregada': False}}, upsert=True)

def ejecutar_saga(d, ev, reloj):
    sid, nodo, p = ev['message_id'], ev['node_id'], ev['payload']
    previa = d.sagas.find_one({'saga_id': sid})
    if previa and previa['estado'] in ('COMPLETADA', 'COMPENSADA'):
        return
    lease = lease_vigente(d)
    if not lease:
        raise RuntimeError('sin lease: no se ejecuta la saga')
    d.sagas.update_one({'saga_id': sid}, {'$set': {'estado': 'EN_CURSO', 'origen': nodo, 'recurso': p['recurso'], 'cantidad': p['cantidad'], 'lamport': ev['lamport']}, '$setOnInsert': {'pasos': []}}, upsert=True)
    hechos = []
    try:
        _paso(d, sid, '1_RESERVAR', 'INICIADO'); _falla(p, 1)
        reservar(d, sid, nodo, p); hechos.append(1); _paso(d, sid, '1_RESERVAR', 'OK')
        _paso(d, sid, '2_REPLICAR', 'INICIADO'); _falla(p, 2)
        replicar(d, sid, nodo, p, lease); hechos.append(2); _paso(d, sid, '2_REPLICAR', 'OK')
        _paso(d, sid, '3_CONFIRMAR', 'INICIADO'); _falla(p, 3)
        confirmar(d, sid, nodo, p); _paso(d, sid, '3_CONFIRMAR', 'OK')
        d.sagas.update_one({'saga_id': sid}, {'$set': {'estado': 'COMPLETADA'}})
    except Exception as e:
        log.warning('Saga %s: %s; compensando', sid, e)
        if 2 in hechos:
            anular_replica(d, sid, nodo); _paso(d, sid, '2_REPLICAR', 'COMPENSADO')
        if 1 in hechos:
            liberar(d, sid); _paso(d, sid, '1_RESERVAR', 'COMPENSADO')
        d.sagas.update_one({'saga_id': sid}, {'$set': {'estado': 'COMPENSADA', 'error': str(e)}})
