import os, sys, time, subprocess
sys.path.insert(0, os.getcwd())
import requests
from common.db import bd

d = bd()
res = []

def chk(nombre, cond):
    res.append(bool(cond))
    print('[OK]    ' if cond else '[FALLA] ', nombre)

srv = subprocess.run(
    ['docker', 'compose', 'ps', '--status', 'running', '--services'],
    capture_output=True, text=True
).stdout.split()

chk('1. bully, coordinador y agente corren en contenedores',
    {'bully', 'coordinador', 'agente'} <= set(srv))

chk('2. un agente reporta métricas (últimos 30 s)',
    d.metricas.count_documents({'ts': {'$gt': time.time() - 30}}) >= 1)

try:
    url = 'http://%s:15672/api/queues' % os.environ['BROKER_HOST']
    colas = requests.get(
        url,
        auth=(os.environ['BROKER_USER'], os.environ['BROKER_PASS']),
        timeout=3
    ).json()
    prop = [c for c in colas if c['name'] in ('metrics', 'allocations', 'failover')]
    chk('3. broker: 3 colas y todos los mensajes confirmados',
        len(prop) == 3 and all(c['messages'] == 0 for c in prop))
except Exception as e:
    chk('3. broker alcanzable: ' + str(e), False)

chk('3b. eventos procesados desde el broker',
    d.eventos_log.count_documents({}) > 0)

ok_ids = [s['saga_id'] for s in d.sagas.find({'estado': 'COMPLETADA'})]
chk('4. asignación completa de extremo a extremo', len(ok_ids) >= 1)

inv = d.inventario.find_one({'_id': 'inventario_global'})
chk('5. la BD replicada registra asignaciones',
    any(h.get('saga_id') in ok_ids for h in inv.get('historial_asignaciones', [])))

from coordinador import saga
chk('6. Saga con compensaciones definidas',
    hasattr(saga, 'liberar') and hasattr(saga, 'anular_replica'))

chk('6b. existe una Saga COMPENSADA de prueba',
    d.sagas.count_documents({'estado': 'COMPENSADA'}) >= 1)

chk('7. docker compose valida el repositorio',
    subprocess.run(['docker', 'compose', 'config', '-q']).returncode == 0)

print('RESULTADO:', 'LISTO PARA EL 90 %' if all(res) else 'FALTAN PUNTOS')
