# Tarea 3 — Beam avanzado

[![Validación](https://github.com/acuevas-hue/streaming-fpuna-clase6-tarea/actions/workflows/ci.yml/badge.svg)](https://github.com/acuevas-hue/streaming-fpuna-clase6-tarea/actions/workflows/ci.yml)

**Autor de la entrega:** [acuevas-hue](https://github.com/acuevas-hue)

Solución reproducible para la Tarea 3 de **Streaming de datos y sus aplicaciones**. El notebook implementa tiempo de evento, ventanas, lateness, deduplicación con estado y timer, triggers y un sink idempotente.

## Ejecución rápida con uv

Requiere Python 3.12 y [uv](https://docs.astral.sh/uv/).

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

Marimo queda disponible en la dirección indicada por la terminal.

## Ejecución con Docker

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. El editor usa `--no-token` únicamente para desarrollo local y no debe exponerse a una red pública.

Para abrir el notebook en modo de solo ejecución:

```bash
docker compose --profile view up --build viewer
```

y abrir <http://localhost:2719>.

## Validación

```bash
uv run pytest -v
uv run ruff check .
uv run marimo check --strict notebook.py
docker build -t clase6-beam-tarea .
```

El workflow de GitHub Actions ejecuta estos controles en cada `push` y `pull_request`; su estado queda visible en el badge superior.

## Contrato implementado

- `event_time` se convierte a UTC y determina la ventana, sin depender del orden de llegada.
- Se utilizan ventanas fijas `[inicio, fin)` de 60 segundos.
- Solo se agregan eventos con estado `CONFIRMED`.
- El atraso se calcula como `arrival_time - event_time`; el límite predeterminado es 120 segundos.
- Los eventos se deduplican por `event_id` con estado aislado por comercio y ventana.
- Un timer de event time elimina el estado en `window_end + allowed_lateness`.
- La política utiliza `AfterWatermark`, un pane early por processing time, revisiones late y modo `ACCUMULATING`.
- La salida se materializa mediante UPSERT con clave `merchant_id|window_start`.

## Resultado del dataset provisto

Con ventanas de 60 segundos y allowed lateness de 120 segundos se leen 9 eventos, se aceptan 5 y se producen 4 totales:

| Comercio | Ventana UTC | Total |
|---|---|---:|
| `m-azul` | 13:00–13:01 | 170000 |
| `m-azul` | 13:02–13:03 | 200000 |
| `m-verde` | 13:00–13:01 | 80000 |
| `m-verde` | 13:01–13:02 | 90000 |

`p-003` y `p-008` no están confirmados, la segunda aparición de `p-002` es duplicada y `p-007` llega 169 segundos tarde, por encima del límite de 120 segundos. Con una tolerancia de 180 segundos, `p-007` es aceptado como revisión tardía.

## Decisiones y trade-offs

### Tiempo y completitud

Usar event time conserva la ventana real del pago cuando los eventos llegan desordenados. Aumentar allowed lateness recuperaría más eventos tardíos, pero mantendría estado durante más tiempo y aumentaría el costo.

### Estado y expiración

Beam mantiene `seen_ids` por clave y ventana. El timer asociado al watermark limpia ese estado cuando ya no pueden aceptarse eventos válidos. Sin expiración, el conjunto crecería indefinidamente.

### Triggers

Los panes early reducen la latencia de visualización, pero son provisionales. El pane on-time aparece cuando el watermark alcanza el final de la ventana y los panes late corrigen el resultado. `ACCUMULATING` permite que cada emisión represente el total conocido, simplificando el consumo.

### Idempotencia

Un sink append-only duplica filas cuando hay reintentos. El UPSERT conserva una sola entidad lógica por comercio y ventana. Este contrato supone que cada revisión reemplaza el total anterior para esa misma clave.

## Pruebas cubiertas

La suite provista permanece sin modificaciones y se complementa con casos límite para timestamps inválidos, parámetros incorrectos, revisiones idempotentes, deduplicación stateful y un escenario temporal con `TestStream` que incorpora un evento late permitido.

El archivo `data/payments.jsonl` permanece idéntico al del proyecto base.
