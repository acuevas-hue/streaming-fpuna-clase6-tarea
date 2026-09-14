# Tarea 3 — Beam avanzado

Solución autocontenida para la **Tarea 3** de la asignatura *Streaming de datos
y sus aplicaciones*. `notebook.py` implementa el procesamiento determinista,
el pipeline de Apache Beam, la deduplicación con estado y timer, una política
de triggers y la simulación de un sink idempotente.

## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.

El editor usa `--no-token` para simplificar el trabajo en `localhost`; no debe
exponerse directamente a una red pública.

## Ejecutar con uv

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

## Trabajar con tests

```bash
uv run pytest
```

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Decisiones y trade-offs

### Tiempo, ventanas y lateness

- Los timestamps terminados en `Z` se convierten en `datetime` conscientes de
  UTC y Beam recibe cada evento mediante `TimestampedValue`.
- Los totales usan ventanas fijas semiabiertas `[inicio, fin)` de 60 segundos.
  Por eso el orden de llegada no cambia la ventana de un pago.
- La versión determinista calcula el atraso como `arrival_time - event_time`.
  Un pago confirmado que llega después del cierre, pero dentro de la tolerancia,
  se acepta como revisión; uno con más de 120 segundos se conserva en auditoría
  con `reason="too_late"` y no altera el total.
- La política streaming usa `AfterWatermark`, una estimación temprana a los 30
  segundos de processing time, revisiones tardías por elemento y panes
  acumulativos. Acumular facilita emitir revisiones completas, a cambio de
  repetir valores que el consumidor debe sobrescribir idempotentemente.

### Estado y deduplicación

Antes del `DoFn` con estado, los eventos se convierten a pares cuya clave es
`merchant_id`. Así, un mismo `event_id` en dos comercios no colisiona. El
`SetStateSpec` retiene los identificadores vistos por clave y ventana; un timer
de event time se programa para `window.end + allowed_lateness` y limpia el
estado. Sin ese timer, las claves procesadas quedarían retenidas y el uso de
memoria crecería indefinidamente.

### Efectos externos e idempotencia

La clave lógica del resultado es `merchant_id|window_start`. La simulación
append-only ejecuta un `POST` por intento y por eso materializa duplicados. El
modo idempotente ejecuta conceptualmente un `UPSERT` sobre un diccionario: los
reintentos reemplazan la misma entidad y convergen a una única fila. En un sink
real, la operación y la restricción de unicidad deben implementarse de forma
atómica.

## Evidencia reproducible

Con el dataset entregado y la configuración predeterminada se procesan **9
eventos**: **5 se aceptan** y se producen **4 totales**. Los otros cuatro son
un pago pendiente, uno rechazado, un duplicado y un pago que excede el atraso
permitido. Entre los resultados relevantes están:

| Comercio | Ventana UTC | Total |
|---|---|---:|
| `m-azul` | `13:00–13:01` | 170000 |
| `m-verde` | `13:00–13:01` | 80000 |
| `m-verde` | `13:01–13:02` | 90000 |
| `m-azul` | `13:02–13:03` | 200000 |

La validación completa se reproduce con:

```bash
uv run pytest
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

## Entrega

Entregar un repositorio propio que incluya:

- `notebook.py` con todas las funciones implementadas;
- evidencia de ejecución del pipeline;
- todas las pruebas provistas para desorden, duplicados, atraso y reintentos
  ejecutadas y aprobadas;
- un README breve con decisiones y trade-offs;
- instrucciones reproducibles con Docker o `uv`.

No modificar `data/payments.jsonl`; puede agregarse un conjunto de datos
adicional para las pruebas.
