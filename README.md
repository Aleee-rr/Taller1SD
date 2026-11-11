# Taller1SD

# Taller 1: Coordinación y Datos del Sistema:
**Asignatura:** Sistemas Distribuidos  
**Programa:** Ingeniería en Telemática - Universidad Distrital Francisco José de Caldas  
**Docente:** Msc. Ing. Óscar Gabriel Espejo Mojica  

---

## Integrantes del Grupo:

| Johan Sebastián López Barrera | [20262678088] | Red y Replicación de Datos |
| [2] | [Código] | Algoritmos de Coordinación |
| [3] | [Código] | Control de Concurrencia |
| Cristhopher Malagon Gonzalez | [20262678035] | Pruebas de Partición |
| [Edy Alexandra Izquierdo Rodríguez] | [20262678092] | Análisis CAP y Documentación 

## Estructura de despliegue

- `app/nodo.py`: aplicacion Bully con integracion MongoDB (codigo existente en revision).
- `legacy/NODO.py`: version original sin integracion MongoDB, solo para referencia.
- `config/peers.env`: mapa de IPs Tailscale y puerto TCP 5050.
- `scripts/instalar_dependencias.sh`: instala PyMongo en un entorno Python local.
- `scripts/iniciar_nodo.sh`: detecta el hostname y ejecuta el mismo codigo en los tres nodos.
- `scripts/distribuir.sh`: transfiere el ultimo commit local por SSH desde Node1.
- `scripts/estado.sh`: consulta el estado Bully de un nodo.
- `docs/DESPLIEGUE.md`: instrucciones y advertencias.

**No se incluyen credenciales ni claves SSH.** La configuracion de MongoDB y las pruebas de
fallos/particiones son actividades separadas; consultar el taller antes de afirmar que pasan.
