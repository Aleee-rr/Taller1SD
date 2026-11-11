# Despliegue de Taller1SD

## Red
- Node1: 100.103.197.36 (PC principal, SSH Windows 2222)
- Node2: 100.109.167.107 (PC principal, SSH Windows 2223)
- Node3: 100.104.202.47 (PC compañero, SSH Windows 2224)
- SSH Ubuntu a Ubuntu: puerto 22 por Tailscale
- Aplicacion Python: TCP 5050 por Tailscale
- MongoDB: TCP 27017; replica set rs-inventario

## Preparar repositorio desde Node1
1. Reorganizar con `organizar_taller1sd.sh` una sola vez y revisar `git status`.
2. `git add -A && git commit -m "Organiza despliegue de tres nodos"` **solo dentro de** `~/Taller1SD`.
3. Publicar con `git push origin main` solamente si tienes permisos.
4. `bash scripts/instalar_dependencias.sh` en Node1.

## Distribuir desde Node1
- `bash scripts/distribuir.sh node2`
- `bash scripts/distribuir.sh node3`

Se copia el commit local por SSH usando la clave privada de Node1. No incluye `.git`, claves SSH,
logs ni entornos virtuales. Si `~/Taller1SD` existe en un destino sin marcador de despliegue,
el script se detiene para proteger esos archivos.

## En cada nodo
- `cd ~/Taller1SD && bash scripts/instalar_dependencias.sh`
- `bash scripts/iniciar_nodo.sh` (deja la consola abierta)
- En otra consola: `bash scripts/estado.sh node1` / `node2` / `node3`

**Estado:** Esto prepara y distribuye el codigo existente, pero NO demuestra que el Bully
resuelva todas las elecciones o conflictos, ni que MongoDB tenga ya tres miembros.
No hacer pruebas de escritura de inventario o particion antes de revisarlas.
