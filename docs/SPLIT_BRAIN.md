# Control de split-brain: Bully + MongoDB lease/fencing

## Dos etapas independientes

- **Etapa 1: infraestructura (no tocar):** Ubuntu, VirtualBox, claves SSH, Tailscale, MongoDB como servicio del sistema.
- **Etapa 2: aplicacion reproducible:** clonar el repositorio, instalar dependencias y ejecutar el mismo `app/nodo.py` en los tres nodos.

## Problema del codigo anterior

1. El `term` se incrementaba localmente; dos nodos podian proclamarse lideres.
2. Un proceso recuperado podia tener un `term` inferior al de un lider de menor ID.
3. `ASSIGN` podia responder `ASSIGNED` tras fallar la escritura en MongoDB.
4. No existia un token de fencing persistente para bloquear escritores obsoletos.

## Cambio introducido

1. **Bully:** cada nodo consulta a los identificadores superiores (`ELECTION`/`OK`).
2. **Autorizacion:** el candidato solo pasa a `LIDER` al adquirir un lease en `recursos_borde.coordinacion`, con confirmacion `w=majority`. MongoDB mantiene `epoch` creciente y un `token` aleatorio.
3. **Heartbeat:** los latidos incluyen `token` y `epoch`; quien los recibe valida que MongoDB reconozca el lease. Si se pierde la mayoria, el lider abandona su estado activo cuando no puede renovar.
4. **Recuperacion:** cuando reaparece un nodo de ID mayor, solicita una cesion con prueba de retorno sobre la IP Tailscale; el lider de ID menor deja vencer o libera su lease. El nuevo lider necesita reclamarlo en MongoDB.
5. **Fencing:** `ASSIGN` toca el registro de lease y el inventario en una unica transaccion MongoDB. Si el lease fue reemplazado o esta vencido, **no** confirma la asignacion. Solo `ASSIGNED` significa commit confirmado.

## Condiciones y limites

- La autoridad usa MongoDB como servicio independiente del coordinador Bully. El MongoDB `PRIMARY` y el lider Bully pueden ser nodos distintos.
- Para tolerar la perdida de un miembro MongoDB, configurar un **replica set de tres miembros con voto**. Una mayoria de tres equivale a dos votos. Antes de completarlo, una particion puede bloquear todas las escrituras (comportamiento seguro).
- Durante una particion, un nodo puede creer localmente que es lider por un breve periodo, pero no puede confirmar un `ASSIGN` con fencing obsoleto. Es una distincion entre liderazgo observado y autoridad de escritura.
- La implementacion es de laboratorio. Usa relojes de pared UTC para el vencimiento del lease: sincronizar relojes (NTP). Para despliegues de produccion hacen falta pruebas rigurosas de failover, autenticacion de mensajes, observabilidad e idempotencia de operaciones.
- `STATUS.can_assign` consulta la validez del lease en el momento de consultar. Toda asignacion debe validar **de nuevo**, dentro de la transaccion.
- Para el requisito de escrituras concurrentes de la guia, las operaciones *tentativas* durante una particion deben registrarse aparte, con vectores de version y reconciliacion posterior; no permitir que operaciones sin mayoria se presenten como confirmadas. Esta parte queda para la siguiente etapa.
- El protocolo TCP no incluye autenticacion criptografica de mensajes; limitar puerto 5050 a la red Tailscale y proteger las claves SSH.

## Tests (sin alterar MongoDB)

```bash
cd ~/Taller1SD
.venv/bin/python -m unittest discover -s tests -v
```

Casos: eleccion inicial 3, caida del proceso 3, eleccion de 2, recuperacion de 3, rechazo de coordinador falsificado de ID menor y rechazo de asignacion al perder el lease.

## Integracion real (posterior)

1. Verificar con `mongosh` el `rs.status()` y los miembros; no correr `rs.initiate()` de nuevo.
2. Iniciar `bash scripts/iniciar_nodo.sh` en cada nodo y dejarlo abierto.
3. Consultar `bash scripts/estado.sh node1`, `node2` y `node3`: el lider legitimo debe tener `can_assign=true`.
4. Simular caida con `Ctrl+C` solo en la aplicacion del nodo 3. Confirmar recuperacion de 2 luego del vencimiento del lease.
5. Reiniciar 3. Confirmar retorno estable al ID mayor y que nunca existan dos nodos con `can_assign=true` simultaneamente.
6. Para particiones de red usar reglas precisas `iptables` y una forma separada de recuperacion (consola VirtualBox). No aislar SSH inadvertidamente.

## Git

Publicar el cambio desde una copia de Git en Node1. Los otros nodos deben poder recrearse desde `git clone` (no se copia `.venv`, claves privadas ni logs).
