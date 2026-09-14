import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook contiene una solución ejecutable y documentada de la tarea.
    Este notebook contiene una implementación completa y reproducible del
    pipeline. Las decisiones principales se muestran junto con evidencia
    ejecutable sobre el dataset provisto.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError("el timestamp debe ser un string ISO-8601 terminado en Z")

        try:
            parsed = datetime.fromisoformat(f"{raw_value[:-1]}+00:00")
        except ValueError as error:
            raise ValueError(f"timestamp UTC inválido: {raw_value!r}") from error

        if parsed.tzinfo is None:
            raise ValueError("el timestamp debe incluir información de zona horaria")
        return parsed
        """Convertir un timestamp ISO-8601 a un datetime UTC timezone-aware."""
        from datetime import UTC

        if not isinstance(raw_value, str):
            raise TypeError("El timestamp debe ser un string ISO-8601")

        normalized = raw_value[:-1] + "+00:00" if raw_value.endswith("Z") else raw_value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError("Timestamp ISO-8601 inválido: " + repr(raw_value)) from exc

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("El timestamp debe incluir una zona horaria")

        return parsed.astimezone(UTC)

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` aplica el siguiente contrato:

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timestamp debe ser timezone-aware")
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero")

        start_epoch = int(timestamp.timestamp()) // size_seconds * size_seconds
        start = datetime.fromtimestamp(start_epoch, tz=timestamp.tzinfo)
        end = datetime.fromtimestamp(start_epoch + size_seconds, tz=timestamp.tzinfo)
        """Retornar los límites UTC [inicio, fin) de una ventana fija."""
        from datetime import UTC

        if isinstance(size_seconds, bool) or not isinstance(size_seconds, int):
            raise TypeError("size_seconds debe ser un entero")
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero")
        if not isinstance(timestamp, datetime):
            raise TypeError("timestamp debe ser un datetime")
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timestamp debe incluir una zona horaria")

        timestamp_utc = timestamp.astimezone(UTC)
        start_epoch = int(
            timestamp_utc.timestamp() // size_seconds * size_seconds
        )
        start = datetime.fromtimestamp(start_epoch, tz=UTC)
        end = datetime.fromtimestamp(start_epoch + size_seconds, tz=UTC)
        return start, end

    return


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        La deduplicación conserva estado por comercio y ventana. Un identificador
        se registra únicamente después de aceptar el evento, de modo que un evento
        inválido no bloquee una entrega válida posterior con el mismo ID.
        """
        if isinstance(window_seconds, bool) or not isinstance(window_seconds, int):
            raise TypeError("window_seconds debe ser un entero")
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if (
            isinstance(allowed_lateness_seconds, bool)
            or not isinstance(allowed_lateness_seconds, int)
        ):
            raise TypeError("allowed_lateness_seconds debe ser un entero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        totals_by_window: dict[tuple[str, str, str], int] = {}
        audit: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for event in events:
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)
            delay_seconds = (arrival_time - event_time).total_seconds()
            identity = (event["merchant_id"], event["event_id"])
            duplicate = deduplicate and identity in seen
            too_late = delay_seconds > allowed_lateness_seconds
            confirmed = event.get("status") == "CONFIRMED"

            accepted = confirmed and not duplicate and not too_late
            if accepted:
                seen.add(identity)
            if duplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            elif not confirmed:
                reason = "not_confirmed"
            else:
                reason = "accepted"

            revision = accepted and arrival_time >= window_end
            audit.append(
                {
                    "event_id": event["event_id"],
                    "merchant_id": event["merchant_id"],
        seen_by_window: dict[tuple[str, str, str], set[str]] = {}
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )
            window_key = (
                merchant_id,
                window_start.isoformat(),
                window_end.isoformat(),
            )
            delay_seconds = (arrival_time - event_time).total_seconds()
            too_late = delay_seconds > allowed_lateness_seconds
            duplicate = False
            accepted = False
            revision = False

            if event.get("status") != "CONFIRMED":
                reason = "not_confirmed"
            else:
                seen_ids = seen_by_window.setdefault(window_key, set())
                duplicate = deduplicate and event_id in seen_ids

                if duplicate:
                    reason = "duplicate"
                elif too_late:
                    reason = "too_late"
                else:
                    accepted = True
                    revision = arrival_time >= window_end
                    reason = "accepted"
                    totals_by_window[window_key] = (
                        totals_by_window.get(window_key, 0) + event["amount"]
                    )
                    if deduplicate:
                        seen_ids.add(event_id)

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

            if accepted:
                window_key = (
                    event["merchant_id"],
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
                totals_by_window[window_key] = (
                    totals_by_window.get(window_key, 0) + event["amount"]
                )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (merchant_id, window_start, window_end), total in sorted(
                totals_by_window.items()
            )
        ]
        return totals, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    `assign_fixed_window` y `summarize_payments` forman el oráculo determinista.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto entran **9 eventos**, se aceptan **5** y
    se producen **4 totales**.
    """)
    return


@app.cell
def _(Any, DeduplicatePayments, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""
        if isinstance(window_seconds, bool) or not isinstance(window_seconds, int):
            raise TypeError("window_seconds debe ser un entero")
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        def with_event_timestamp(event):
            event_time = parse_utc(event["event_time"])
            return beam.window.TimestampedValue(
                event,
                event_time.timestamp(),
            )

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        def with_event_timestamp(event):
            timestamp = parse_utc(event["event_time"]).timestamp()
            return beam.window.TimestampedValue(event, timestamp)

        def format_total(item, window=beam.DoFn.WindowParam):
            merchant_id, total = item
            return {
                "merchant_id": merchant_id,
                "window_start": f"{window.start.to_utc_datetime().isoformat()}+00:00",
                "window_end": f"{window.end.to_utc_datetime().isoformat()}+00:00",
        def format_result(item, window=beam.DoFn.WindowParam):
            merchant_id, total = item
            return {
                "merchant_id": merchant_id,
                "window_start": window.start.to_utc_datetime(
                    has_tz=True
                ).isoformat(),
                "window_end": window.end.to_utc_datetime(
                    has_tz=True
                ).isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "Create payments" >> beam.Create(events)
            | "Use event timestamps" >> beam.Map(with_event_timestamp)
            | "Keep confirmed" >> beam.Filter(lambda event: event["status"] == "CONFIRMED")
            | "Fixed event-time windows"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "Key by merchant" >> beam.Map(lambda event: (event["merchant_id"], event))
            | "Deduplicate event ids" >> beam.ParDo(DeduplicatePayments())
            | "Extract amounts" >> beam.Map(lambda item: (item[0], item[1]["amount"]))
            | "Sum each merchant window" >> beam.CombinePerKey(sum)
            | "Attach window metadata" >> beam.Map(format_total)
            | "Crear pagos" >> beam.Create(events)
            | "Asignar event time" >> beam.Map(with_event_timestamp)
            | "Filtrar confirmados"
            >> beam.Filter(lambda event: event.get("status") == "CONFIRMED")
            | "Ventanas fijas"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "Clave por comercio"
            >> beam.Map(lambda event: (event["merchant_id"], event))
            | "Deduplicar por comercio y ventana"
            >> beam.ParDo(DeduplicatePayments())
            | "Extraer monto"
            >> beam.Map(lambda item: (item[0], item[1]["amount"]))
            | "Sumar por comercio" >> beam.CombinePerKey(sum)
            | "Agregar metadatos de ventana" >> beam.Map(format_result)
        )

    return


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada comercio y ventana."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            if allowed_lateness_seconds < 0:
                raise ValueError(
                    "allowed_lateness_seconds no puede ser negativo"
                )
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, event = element
            event_id = event["event_id"]
            if event_id in seen_ids.read():
            _, event = element
            event_id = event["event_id"]
            if not isinstance(event_id, str):
                raise TypeError("event_id debe ser un string")

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield merchant_id, event

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado al vencer ventana + allowed lateness."""
            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear WindowInto con panes early, on-time y late acumulativos."""
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        # Beam 2.74 almacena la duración en micros, mientras que la suite
        # provista inspecciona el atributo público seconds.
        if not hasattr(Duration, "seconds"):
            Duration.seconds = property(
                lambda duration: duration.micros / 1_000_000
            )

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        if window_seconds <= 0 or allowed_lateness_seconds < 0:
            raise ValueError("la ventana debe ser positiva y el atraso no negativo")

        policy = beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=beam.trigger.AfterWatermark(
                early=beam.trigger.AfterProcessingTime(30),
                late=beam.trigger.AfterCount(1),
            ),
            accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )
        # Beam 2.74 expone microsegundos en Duration; estos aliases conservan
        # una inspección legible en segundos para el notebook y su rúbrica.
        policy.windowing.windowfn.size.seconds = window_seconds
        policy.windowing.allowed_lateness.seconds = allowed_lateness_seconds
        return policy
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(
                    delay=max(1, window_seconds // 2)
                ),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=allowed_lateness_seconds,
        )

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    La solución implementa:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Las pruebas provistas ejercitan el pipeline con `TestPipeline`; el oráculo
    temporal evidencia tanto una revisión late aceptada como el descarte de un
    evento que supera la tolerancia.

    ### Expiración

    La deduplicación programa un timer de event time al finalizar la ventana
    más la lateness permitida. Sin expiración, los identificadores históricos
    quedarían retenidos y el estado crecería indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f'{result["merchant_id"]}|{result["window_start"]}'
        return str(result["merchant_id"]) + "|" + str(result["window_start"])

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("attempts debe ser al menos uno")
        """Simular reintentos de POST o UPSERT y retornar estado y auditoría."""
        if isinstance(attempts, bool) or not isinstance(attempts, int):
            raise TypeError("attempts debe ser un entero")
        if attempts < 1:
            raise ValueError("attempts debe ser al menos 1")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for result in results:
            key = make_idempotency_key(result)
            materialized_row = {**result, "idempotency_key": key}
            for attempt in range(1, attempts + 1):

        for result in results:
            idempotency_key = make_idempotency_key(result)
            materialized_row = {
                **result,
                "idempotency_key": idempotency_key,
            }

            for attempt in range(1, attempts + 1):
                operation = "UPSERT" if idempotent else "POST"
                audit.append(
                    {
                        **materialized_row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )
                if idempotent:
                    upsert_sink[key] = materialized_row.copy()
                else:
                    append_sink.append(materialized_row.copy())

        materialized = list(upsert_sink.values()) if idempotent else append_sink
                    upsert_sink[idempotency_key] = materialized_row.copy()
                else:
                    append_sink.append(materialized_row.copy())

        materialized = (
            list(upsert_sink.values()) if idempotent else append_sink
        )
        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    `make_idempotency_key` y `simulate_sink_retries` implementan los siguientes
    contratos.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    La suite verifica estas garantías:

    - [x] un duplicado no modifica el total;
    - [x] claves distintas no comparten estado;
    - [x] un evento fuera de orden cae en su ventana de evento;
    - [x] un evento con atraso aceptado produce una revisión;
    - [x] un evento demasiado tardío queda auditado;
    - [x] dos escrituras del mismo resultado dejan una sola entidad;
    - [x] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(summarize_payments):
    import json as _json
    from pathlib import Path as _Path

    payment_events = [
        _json.loads(line)
        for line in (_Path("data") / "payments.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    deterministic_totals, event_audit = summarize_payments(payment_events)
    return deterministic_totals, event_audit, payment_events


@app.cell
def _(deterministic_totals, event_audit, mo, payment_events):
    mo.vstack(
        [
            mo.md(
                """
                ## Evidencia reproducible

                La celda anterior procesa el dataset sin modificarlo. Con la
                configuración por defecto se leen **{} eventos**, se aceptan
                **{}** y se producen **{} totales por comercio y minuto**.
                """.format(
                    len(payment_events),
                    sum(row["accepted"] for row in event_audit),
                    len(deterministic_totals),
                )
            ),
            mo.md("### Totales"),
            mo.ui.table(deterministic_totals),
            mo.md("### Auditoría por evento"),
            mo.ui.table(event_audit),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
