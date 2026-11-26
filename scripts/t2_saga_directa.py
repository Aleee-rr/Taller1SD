import sys
import os

sys.path.insert(0, os.getcwd())

from common.db import bd, asegurar_indices
from common.lamport import LamportClock
from common.mq import nuevo_evento
from coordinador.saga import ejecutar_saga

falla = int(sys.argv[1]) if len(sys.argv) > 1 else None

d = bd()
asegurar_indices(d)

ev = nuevo_evento(
    'allocation_request',
    3,
    1,
    {
        'recurso': 'memoria_mb',
        'cantidad': 128,
        'falla_en_paso': falla
    }
)

ejecutar_saga(d, ev, LamportClock())

s = d.sagas.find_one(
    {'saga_id': ev['message_id']},
    {'_id': 0}
)

print("\nID:", ev['message_id'])
print("Estado:", s['estado'])
print("Pasos:")

for paso in s['pasos']:
    print(paso['paso'], paso['estado'])

print("Error:", s.get('error'))
