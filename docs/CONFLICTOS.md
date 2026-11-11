# Fase 3 — Control de conflictos (Taller 1)

## Alcance

El codigo de `app/nodo.py` permanece intacto. Sus asignaciones `ASSIGNED` exigen
lease/fencing y `w=majority`; una propuesta sin confirmacion no puede presentarse
como una asignacion confirmada. Este componente captura cambios **TENTATIVOS**
en un archivo local y los reconcilia despues en MongoDB mediante el coordinador
Bully autorizado.

## Modelo de datos

- Inventario confirmado: `recursos_borde.inventario` (documento `_id: inventario_global`).
- Vector de version: `n1`, `n2`, `n3`.
- Snapshot y propuestas locales: `logs/conflictos/*.json` (no publicados por Git).
- Auditoria confirmada: `recursos_borde.resoluciones_conflicto`.
- Contadores: `recursos_borde.metricas_conflictos` (`_id: taller1`).

## Comandos

Desde el directorio raiz del repositorio:

```bash
# En cada nodo, ANTES de una particion (tres MongoDB sanos):
bash scripts/conflictos.sh snapshot

# En Node 2 y Node 3 DURANTE la simulacion / particion;
# no necesita acceso a MongoDB, pero si que el Bully local este escuchando.
bash scripts/conflictos.sh propose --resource nodo_3.cpu_cores --value 1  # Node 2
bash scripts/conflictos.sh propose --resource nodo_3.cpu_cores --value 0  # Node 3

# DESPUES de reunir ambos archivos en el nodo coordinador legitimo:
bash scripts/conflictos.sh compare --a /ruta/node2.json --b /ruta/node3.json
bash scripts/conflictos.sh reconcile --a /ruta/node2.json --b /ruta/node3.json
bash scripts/conflictos.sh metrics
```

Los archivos de propuesta se imprimen en la salida de `propose`. Copiarlos
solo despues de recuperar la conectividad. La reconciliacion valida que el
proceso Bully local sea el lider, vuelve a comprobar el lease dentro de una
transaccion MongoDB, confirma con `w=majority`, y evita aplicar dos veces el
mismo par de operaciones con un ID determinista.

## Regla y limitaciones

1. Dos versiones son concurrentes si ninguna domina la otra.
2. Gana la mayor marca Lamport; en empate, el nodo con menor ID.
3. Se cuenta 1 conflicto detectado y 1 resuelto por par realmente concurrente
   confirmado. Los contadores y auditoria se actualizan en la misma transaccion.
4. Si el valor confirmado de ese recurso cambio desde el snapshot, **se rechaza**
   la resolucion automatica; requiere revision. No se pierde una actualizacion
   confirmada posterior.
5. Se admiten dos propuestas sobre un mismo recurso desde un mismo snapshot.
   Un registro de muchas operaciones requiere una estrategia de reconciliacion
   incremental adicional; este modulo es para el escenario del taller.
6. Son propuestas locales sin confirmar, no dos escrituras confirmadas en
   MongoDB. Las asignaciones reales permanecen bajo lease/fencing.
7. No editar manualmente los snapshots ni ejecutar `reconcile` en un seguidor.
8. No borrar `/var/lib/mongodb`, no repetir `rs.initiate()`, no abrir el puerto
   27017 a Internet y no bloquear SSH/Tailscale en las pruebas de particion.

## Pruebas sin tocar MongoDB

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_conflictos.py' -v
```
