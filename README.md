# Taller 1 — Coordinación y datos del sistema

**Asignatura:** Sistemas Distribuidos Modernos  
**Programa:** Ingeniería en Telemática — Universidad Distrital Francisco José de Caldas  
**Docente:** Msc. Ing. Óscar Gabriel Espejo Mojica  
**Repositorio:** [Aleee-rr/Taller1SD](https://github.com/Aleee-rr/Taller1SD)  
**Pruebas documentadas:** 11 de noviembre de 2026

## 1. Descripción del proyecto

Este laboratorio implementa una capa distribuida de **coordinación e inventario de recursos de borde** sobre tres máquinas virtuales Ubuntu Server conectadas por Tailscale. El sistema combina:

- **Algoritmo Bully (matón):** elección del nodo con mayor identificador disponible, detección de fallos mediante latidos y recuperación del coordinador.
- **MongoDB Replica Set:** inventario de CPU, memoria y ancho de banda replicado entre tres miembros.
- **Lease, epoch y fencing:** validación de la autoridad del coordinador para confirmar asignaciones; las escrituras utilizan transacciones con `writeConcern: majority`.
- **Relojes lógicos:** marcas de Lamport para mensajes y propuestas; vectores de versión para identificar concurrencia.
- **Reconciliación de conflictos:** registro de propuestas tentativas durante una partición, detección de versiones concurrentes y resolución determinista.
- **Experimento CAP:** aislamiento de un nodo con `iptables`, observación del quórum, rechazo de escrituras minoritarias y convergencia posterior.

> **Alcance:** es un prototipo académico. Las solicitudes `ASSIGN` registran eventos de asignación y actualizan el vector del inventario; no implementan un planificador de consumo físico de CPU, memoria o red. La modificación de valores del inventario se realiza mediante la reconciliación autorizada.

## 2. Integrantes

| Integrante | Código | Responsabilidad registrada |
|---|---|---|
| Johan Sebastián López Barrera | 20262678088 | Red y replicación de datos |
| Cristhopher Malagon Gonzalez | 20262678035 | Pruebas de partición |
| Edy Alexandra Izquierdo Rodríguez | 20262678092 | Análisis CAP y documentación |
| Camilo Alejandro Contreras Morales | 20262678038 | Liderazgo técnico, arquitectura, desarrollo e integración del sistema distribuido |



## 3. Arquitectura e infraestructura

| Nodo | Hostname | IP Tailscale utilizada | Aplicación |
|---|---|---|---|
| Node 1 | `sdm-node1` | `100.103.197.36` | Bully ID 1 + MongoDB |
| Node 2 | `sdm-node2` | `100.109.167.107` | Bully ID 2 + MongoDB |
| Node 3 | `sdm-node3` | `100.104.202.47` | Bully ID 3 + MongoDB |

Los tres nodos ejecutan el mismo código Python. Se comunican mediante **TCP 5050** (Bully) y **TCP 27017** (MongoDB). El acceso administrativo se realiza por SSH sobre Tailscale. Las direcciones anteriores corresponden al entorno del laboratorio y deben adaptarse si cambia la tailnet.

```text
                 Tailnet (Tailscale)
        ┌──────────────────────────────┐
        │ Node 1  <──> Node 2 <──> Node 3 │
        │ Bully 1      Bully 2    Bully 3 │
        │ MongoDB      MongoDB    MongoDB │
        └──────────────────────────────┘
                  rs-inventario
             3 miembros / mayoría = 2
```

El **líder Bully** y el **PRIMARY de MongoDB** son conceptos independientes: el primero coordina la aplicación; el segundo gestiona las escrituras del replica set. En las pruebas, Node 3 recuperó el liderazgo Bully, mientras que MongoDB podía mantener a Node 1 como `PRIMARY`.

### Elección de coordinador

El algoritmo Bully da prioridad al identificador más alto que pueda obtener autorización. Un candidato solo se convierte en `LIDER` cuando adquiere un lease confirmado por MongoDB. La colección `recursos_borde.coordinacion` conserva `owner`, `token`, `epoch` y `lease_until`.

- Los seguidores detectan la ausencia de latidos e inician elecciones.
- Cuando Node 3 falla, Node 2 puede asumir el liderazgo si conserva el quórum.
- Cuando Node 3 reaparece, solicita la cesión del coordinador de menor ID y adquiere un nuevo lease.
- Cada asignación valida nuevamente el lease y el token de *fencing* dentro de la misma transacción que modifica el inventario. Un nodo sin autorización responde `NOT_LEADER` o `REJECTED`, no `ASSIGNED`.

### Inventario replicado y modelo de consistencia

Se utiliza la base de datos **`recursos_borde`** con las siguientes colecciones:

| Colección | Propósito |
|---|---|
| `coordinacion` | Autoridad Bully: propietario del lease, token, epoch y vencimiento |
| `inventario` | Recursos por nodo (`cpu_cores`, `memoria_mb`, `ancho_banda_mbps`), vector de versión e historial de asignaciones |
| `resoluciones_conflicto` | Auditoría de conflictos confirmados y decisión aplicada |
| `metricas_conflictos` | Contadores de conflictos detectados y resueltos |

Las escrituras autorizadas se realizan con transacciones MongoDB y `writeConcern: majority` (al menos **2 de 3** miembros). El acceso a datos confirmados en los componentes Python utiliza `readConcern: majority`; para verificar replicación se realizaron lecturas locales de miembros `PRIMARY` y `SECONDARY` con `secondaryPreferred`, por lo que una réplica secundaria puede presentar retraso temporal hasta sincronizarse.

## 4. Control de concurrencia

Cada versión tiene las dimensiones `n1`, `n2` y `n3`. Para dos vectores **A** y **B**:

- A domina B si todos sus contadores son mayores o iguales y al menos uno es estrictamente mayor.
- Dos versiones son **concurrentes** si ninguna domina a la otra.
- En concurrencia, gana la propuesta con **mayor reloj de Lamport**; si hay empate, gana el **menor ID de nodo**.

Las propuestas se almacenan primero como archivos JSON **tentativos** en `logs/conflictos/`, sin modificar MongoDB. Tras restablecer la conectividad, el coordinador con lease válido puede ejecutar una reconciliación transaccional; se unen los vectores, se registra un nuevo evento causal y se actualizan la auditoría y los contadores. La operación es idempotente para el mismo par de propuestas mediante un `conflicto_id` determinista.

**Limitación importante:** el reconciliador compara **dos propuestas del mismo recurso y snapshot**. No acepta cambios si el valor confirmado del recurso ya fue modificado desde ese snapshot. No se implementó sincronización automática y generalizada de múltiples operaciones sin conexión.

## 5. Instalación y ejecución

### Requisitos

- Tres VM Ubuntu Server con red Tailscale y SSH por clave pública.
- MongoDB instalado y configurado previamente como replica set `rs-inventario` de tres miembros con voto.
- Python 3, `venv`, Git y `mongosh` para diagnóstico.
- Puertos TCP 5050 y 27017 disponibles dentro de la tailnet.

> Los scripts del repositorio **no instalan ni inicializan MongoDB**. La configuración de red, SSH, Tailscale y replica set forma parte de la infraestructura previa. No repetir `rs.initiate()` sobre un replica set existente.

### Clonar e instalar en cada VM

```bash
git clone https://github.com/Aleee-rr/Taller1SD.git
cd Taller1SD
bash scripts/instalar_dependencias.sh
```

Revisar `config/peers.env` para las IP de Tailscale del entorno. El script `scripts/iniciar_nodo.sh` reconoce los hostnames `sdm-node1`, `sdm-node2` y `sdm-node3`.

### Iniciar el coordinador/agente en cada nodo

```bash
cd ~/Taller1SD
bash scripts/iniciar_nodo.sh
```

Mantener una terminal por nodo con el proceso en ejecución. En otra terminal, consultar estados:

```bash
bash scripts/estado.sh node1
bash scripts/estado.sh node2
bash scripts/estado.sh node3
```

El resultado de `STATUS` incluye `state`, `leader`, `epoch`, `lamport`, `assignments` y `can_assign`. Solo el líder legítimo debe mostrar `can_assign: true`.

### Diagnósticos y pruebas unitarias

```bash
# Ver estado del replica set
mongosh --quiet --eval 'rs.status()'

# Ejecutar pruebas de coordinación y concurrencia
.venv/bin/python -m unittest discover -s tests -v

# Consultar contadores de conflicto
bash scripts/conflictos.sh metrics
```

Para generar propuestas tentativas y reconciliarlas, consultar [docs/CONFLICTOS.md](docs/CONFLICTOS.md). Para detalles de despliegue y seguridad del lease, consultar [docs/DESPLIEGUE.md](docs/DESPLIEGUE.md) y [docs/SPLIT_BRAIN.md](docs/SPLIT_BRAIN.md).

## 6. Experimentos ejecutados y resultados

Las siguientes observaciones proceden de las pruebas de integración realizadas en las tres máquinas virtuales el **11 de noviembre de 2026**.

| Experimento | Comportamiento observado |
|---|---|
| Elección inicial Bully | Node 3 obtuvo el liderazgo, `epoch = 2` |
| Fallo del proceso Bully en Node 3 | Node 2 obtuvo el liderazgo, `epoch = 3`, y confirmó una asignación |
| Recuperación de Node 3 | Node 3 recuperó el liderazgo, `epoch = 4`; Node 2 respondió `NOT_LEADER` |
| Primer conflicto concurrente (CPU) | Ganó Node 3: Lamport `12800` frente a `12679`; `nodo_3.cpu_cores` pasó de 2 a 0 |
| Primera partición de red | Node 3 aislado; Node 1 y Node 2 conservaron quórum (2/3); Node 2 obtuvo `epoch = 5` |
| Reconciliación posterior a partición (memoria) | Ganó Node 2: Lamport `18196` frente a `17683`; `nodo_3.memoria_mb` pasó de 3072 a 2048; coordinador Node 3, `epoch = 6` |
| Segunda partición: prueba CAP | Node 2 confirmó `E4-CAP-MAYORIA-001` en `epoch = 7`; Node 3 devolvió `NOT_LEADER` y no confirmó `E4-CAP-MINORIA-001` |
| Recuperación y convergencia | Las tres réplicas mostraron el mismo inventario y vector final |

### Estado final verificado

```json
{
  "vector_version": {"n1": 0, "n2": 5, "n3": 6},
  "nodo_3": {"cpu_cores": 0, "memoria_mb": 2048},
  "conflictos_detectados": 2,
  "conflictos_resueltos": 2,
  "asignaciones_confirmadas_en_mayoria_durante_CAP": 1,
  "asignaciones_confirmadas_en_minoria_durante_CAP": 0
}
```

## 7. Experimento CAP: interpretación

La partición se indujo en Node 3 mediante reglas `iptables` de entrada y salida sobre los puertos TCP **5050** y **27017**, conservando SSH y el servicio Tailscale. Se programó una restauración automática con `systemd-run` y se verificó la recuperación de conectividad.

| Momento | Node 1 y Node 2 (mayoría) | Node 3 (minoría) | Inventario confirmado |
|---|---|---|---|
| Antes | Comunicación completa; líder Bully Node 3 | Participa normalmente | Replicado en los tres miembros |
| Durante | Node 2 puede ser líder y confirmar escrituras con mayoría | No puede confirmar escrituras sin lease/quórum; `NOT_LEADER` | Se confirma `E4-CAP-MAYORIA-001` y se rechaza la operación minoritaria |
| Después | Recuperan la comunicación y reconocen al coordinador autorizado | Se reincorpora y recupera liderazgo Bully | Las tres réplicas convergen: vector `[0, 5, 6]` |

**Conclusión CAP:** en las rutas de escritura protegidas por MongoDB y fencing, la solución prioriza **consistencia y tolerancia a particiones (CP)**. La mayoría conserva disponibilidad de escritura, mientras que la minoría sacrifica esa disponibilidad para evitar escrituras divergentes confirmadas. Esto no constituye una afirmación de consistencia linealizable para todas las lecturas o interfaces del sistema.

## 8. Evidencias de ejecución

Se organizaron **102 capturas PNG** y cinco registros de consola TXT:

| Evidencias | Contenido | Cantidad |
|---|---|---:|
| [Configuración MongoDB](evidencias/01-configuracion-infraestructura/03-configuracion-mongodb/) | Configuración, replica set, escritura por mayoría y replicación | 10 |
| [Despliegue](evidencias/02-ejecucion-taller-1/01-despliegue/) | Clonado y pruebas del proyecto en las VM | 6 |
| [Elección Bully](evidencias/02-ejecucion-taller-1/02-eleccion-bully/) | Estados, asignaciones, fallo, reelección y recuperación | 32 |
| [Concurrencia](evidencias/02-ejecucion-taller-1/03-control-concurrencia/) | Vectores, propuestas, resolución, métricas y replicación | 22 |
| [Partición y CAP](evidencias/02-ejecucion-taller-1/04-particion-y-cap/) | Antes, durante y después; quórum, rechazos, recuperación y convergencia | 32 |
| [Registros de ejecución](evidencias/02-ejecucion-taller-1/05-registros-ejecucion/) | Logs de Bully y resultados de pruebas unitarias | 5 TXT |

**Capturas clave:** [Reelección de Node 2](evidencias/02-ejecucion-taller-1/02-eleccion-bully/20_Bully_Estados_Reeleccion_Node2.png) · [Conflicto concurrente](evidencias/02-ejecucion-taller-1/03-control-concurrencia/48_Fase3_Deteccion_Conflicto.png) · [Resolución tras partición](evidencias/02-ejecucion-taller-1/04-particion-y-cap/73_Fase4_Conflicto_Resuelto_PostParticion.png) · [Escritura mayoritaria](evidencias/02-ejecucion-taller-1/04-particion-y-cap/80_Fase4_CAP_Asignacion_Mayoria_Node2.png) · [Rechazo minoritario](evidencias/02-ejecucion-taller-1/04-particion-y-cap/81_Fase4_CAP_Rechazo_Minoria_Node3.png) · [Convergencia](evidencias/02-ejecucion-taller-1/04-particion-y-cap/87_Fase4_CAP_Convergencia_Node3.png).

Los directorios [01-conexion-nodos](evidencias/01-configuracion-infraestructura/01-conexion-nodos/) y [02-instalacion-nodos](evidencias/01-configuracion-infraestructura/02-instalacion-nodos/) están reservados para la evidencia específica de infraestructura y verificación inicial de conectividad.

## 9. Estructura del repositorio

```text
app/
  nodo.py             # Elección Bully, heartbeat, lease y asignaciones
  conflictos.py       # Vectores y resolución determinista (funciones puras)
config/
  peers.env           # IP de nodos, puerto de aplicación y URI MongoDB
scripts/
  iniciar_nodo.sh     # Ejecución del nodo identificado por hostname
  estado.sh           # Consulta STATUS
  conflictos.py       # CLI de snapshot, propuestas, comparación y reconciliación
  conflictos.sh       # Envoltorio del CLI con configuración de MongoDB
  instalar_dependencias.sh
  distribuir.sh
tests/
  test_bully.py
  test_conflictos.py
docs/
  DESPLIEGUE.md
  SPLIT_BRAIN.md
  CONFLICTOS.md
evidencias/
  01-configuracion-infraestructura/
  02-ejecucion-taller-1/
legacy/
  NODO.py             # Implementación anterior, conservada como referencia
```

## 10. Limitaciones y trabajo futuro

1. La reconciliación admite pares de propuestas sobre un recurso y un snapshot compartido; una reconciliación general requeriría ordenar y resolver múltiples operaciones tentativas.
2. Las propuestas tentativas se guardan en archivos locales y deben recuperarse y transferirse para su reconciliación; no existe sincronización automática de ese registro.
3. El lease utiliza tiempo UTC del sistema; se requiere sincronización adecuada de relojes entre las VM. La mensajería TCP de Bully no incorpora autenticación criptográfica propia.
4. Las pruebas de partición utilizaron reglas `iptables` con restauración controlada en la VM; no hay todavía un script de partición versionado dentro del repositorio.
5. El sistema es una demostración académica, no una plataforma de producción con monitoreo, cifrado y tolerancia a fallos integrales.

## 11. Bibliografía y software de terceros

- Garcia-Molina, H. (1982). *Elections in a Distributed Computing System*. IEEE Transactions on Computers, C-31(1).
- Fidge, C. J. (1988). *Timestamps in Message-Passing Systems That Preserve the Partial Ordering*.
- Mattern, F. (1989). *Virtual Time and Global States of Distributed Systems*.
- [MongoDB — Replication](https://www.mongodb.com/docs/manual/replication/) y [Write Concern](https://www.mongodb.com/docs/manual/reference/write-concern/).
- [PyMongo](https://pymongo.readthedocs.io/) (`pymongo>=4,<5`): cliente Python de MongoDB utilizado por el proyecto.
- [Tailscale](https://tailscale.com/kb/): conectividad privada entre las máquinas del grupo.
- [Python](https://docs.python.org/3/), [Git](https://git-scm.com/doc), `iptables` y `systemd`: herramientas de ejecución, control de versiones y pruebas de partición.

**Seguridad:** no se publican claves privadas SSH ni credenciales. Los archivos de trabajo locales de `logs/`, el entorno `.venv/` y las claves están excluidos mediante `.gitignore`.
