"""Verifica que ventas-agregadas coincide con las ventas crudas (sin perdida ni doble conteo).

Entrada (stdin): lineas "RAW<TAB>valor" (ventas-raw) y "AGG<TAB>valor" (ventas-agregadas).
Uso tipico (lo hace scripts/prueba-reinicio-streams.sh):
    ... | docker run --rm -i python:3.12-alpine python -c "$(cat scripts/verificar_consistencia.py)" DESDE

Recalcula, a partir de ventas-raw, lo que la app DEBERIA publicar: aplica las mismas reglas de
validacion, deduplica por eventId (se queda con la primera aparicion) y agrupa por tienda y
ventana de 60 s segun eventTime. Compara solo las ventanas que la app ya publico (cerradas) y
cuyo inicio es >= DESDE (ISO-8601). Los eventos descartados por llegar fuera de la gracia no
generan ventana, por eso no aparecen en la comparacion.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

VENTANA_MS = 60_000
desde = sys.argv[1] if len(sys.argv) > 1 else "1970-01-01T00:00:00Z"


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def iso(m):
    return datetime.fromtimestamp(m / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def valida(v):
    try:
        s = json.loads(v)
        if not (s.get("eventId") and s.get("eventTime") and s.get("store")):
            return None
        ms(s["eventTime"])
        q, p, t = s.get("quantity", 0), s.get("unitPriceClp", 0), s.get("totalClp", 0)
        if q <= 0 or p <= 0 or q * p != t:
            return None
        return s
    except Exception:
        return None


esperado = defaultdict(lambda: [0, 0])
vistos = set()
publicado = {}
n_raw = n_val = n_dup = 0
for linea in sys.stdin:
    tipo, _, valor = linea.rstrip("\n").partition("\t")
    if tipo == "RAW":
        n_raw += 1
        s = valida(valor)
        if s is None:
            continue
        n_val += 1
        if s["eventId"] in vistos:
            n_dup += 1
            continue
        vistos.add(s["eventId"])
        inicio = ms(s["eventTime"]) // VENTANA_MS * VENTANA_MS
        esperado[(s["store"], iso(inicio))][0] += 1
        esperado[(s["store"], iso(inicio))][1] += s["totalClp"]
    elif tipo == "AGG":
        a = json.loads(valor)
        publicado[(a["store"], a["window_start"])] = (a["sales_count"], a["total_clp"])

ok, malas = 0, []
for clave, (cnt, tot) in sorted(publicado.items(), key=lambda kv: kv[0][1]):
    if clave[1] < desde:
        continue
    e = tuple(esperado.get(clave, (0, 0)))
    if e == (cnt, tot):
        ok += 1
    else:
        malas.append((clave, e, (cnt, tot)))

print(f"ventas-raw leidos={n_raw} validos={n_val} duplicados_por_eventId={n_dup}")
print(f"ventanas publicadas desde {desde}: {ok + len(malas)}  coinciden={ok}  difieren={len(malas)}")
for (store, w), e, p in malas:
    print(f"  DIFIERE {store} {w}: esperado(conteo,monto)={e} publicado={p}")
print("RESULTADO:", "OK - cada ventana publicada coincide con las ventas crudas" if not malas and ok
      else ("SIN VENTANAS PARA COMPARAR" if not malas else "REVISAR"))
