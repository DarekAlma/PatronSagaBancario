# Orquestación vs. Coreografía — Comparativa

## Resumen

| | Saga Orquestada | Saga Coreografiada |
|---|---|---|
| Coordinador | Sí — `saga_orquestada_flow` (Prefect) | No existe. Cada servicio decide por sí mismo. |
| Quién conoce el flujo completo | El flow de Prefect (`services/prefect-bridge/app/flows.py`) | Nadie: cada servicio solo conoce los eventos a los que reacciona |
| Acoplamiento | Alto acoplamiento temporal al orquestador; bajo acoplamiento entre servicios entre sí (nunca se llaman directamente) | Bajo acoplamiento: los servicios ni siquiera saben cuáles otros existen, solo el contrato de eventos |
| Cómo se dispara una compensación | El flow captura la excepción del paso fallido y llama explícitamente a las tasks de compensación en orden inverso | El servicio que detecta el fallo publica un evento (`RiesgoRechazado`, `TransferenciaFallida`); los demás se suscriben a ese evento y compensan por su cuenta |
| Visibilidad del flujo completo | Un solo flow run en Prefect con el árbol completo de tasks | Varios flow runs independientes en Prefect, correlacionados solo por el tag `sagaId` |
| Punto único de fallo | El propio orquestador (si cae, ninguna saga nueva avanza) | Ninguno — el Event Bus es solo un relé; si un servicio cae, solo se detienen las sagas que dependían de él |
| Facilidad para añadir un paso nuevo | Editar el flow (un solo archivo) | Publicar el nuevo evento y suscribir al servicio interesado, sin tocar a los demás |

## Cómo se implementó cada una en este proyecto

### Orquestada (`services/prefect-bridge/app/flows.py::saga_orquestada_flow`)

Es literalmente **un flow de Prefect**, que es exactamente lo que pide el enunciado ("orquestador central: coordinador o flujo de Prefect"). El flow:

1. Llama a la task `t_debitar_origen`.
2. Si falla (fondos insuficientes) → no hay nada que compensar → `RECHAZADO_FONDOS`.
3. Llama a `t_validar_riesgo`.
4. Si falla → compensa **solo** el paso 1 (`t_compensar_debito`) → `RECHAZADO_RIESGO`.
5. Llama a `t_liquidar_interbancaria`.
6. Si falla → compensa en **orden estrictamente inverso**: primero anula el riesgo (`t_compensar_riesgo`), luego reintegra el débito (`t_compensar_debito`) → `RECHAZADO_RED`.
7. Si todo tiene éxito → `CONFIRMADO`.

Cada llamada a un microservicio de dominio es una `@task` de Prefect: su duración real (incluido el delay de 2-4s simulado dentro del propio microservicio) queda registrada en la UI de Prefect, y una task fallida se ve en rojo de inmediato.

### Coreografiada (eventos vía `services/event-bus`)

No hay ningún componente que conozca la secuencia completa. La secuencia emerge de las suscripciones:

```
TransferenciaSolicitada  → Accounts   → (éxito) SaldoDebitado   / (fallo) DebitoRechazado
SaldoDebitado             → Risk       → (éxito) RiesgoAprobado  / (fallo) RiesgoRechazado
RiesgoAprobado            → Clearing   → (éxito) TransferenciaConfirmada / (fallo) TransferenciaFallida
RiesgoRechazado           → Accounts   → compensa el débito
TransferenciaFallida(RED) → Accounts   → compensa el débito
TransferenciaFallida(RED) → Risk       → anula la aprobación de riesgo
```

El Event Bus (`services/event-bus/src/index.js`) es deliberadamente "tonto": solo tiene un mapa estático `evento → suscriptores` y reenvía por HTTP. No decide nada de negocio ni conoce el estado de la saga. Cada microservicio, al reaccionar, ejecuta su paso a través del Prefect Bridge (`/steps/*`) para que también quede registrado como un flow run independiente en Prefect — así se puede seguir la traza completa de una saga coreografiada filtrando por su `sagaId` en la UI, aunque técnicamente ningún componente individual "sabe" que la saga completa existe.

## Conclusión

Ambas variantes garantizan la misma propiedad de negocio (consistencia eventual, sin dinero perdido ni estados en limbo), pero con trade-offs opuestos: la orquestación centraliza el conocimiento del flujo (más fácil de auditar y depurar, pero crea un punto único de coordinación), mientras que la coreografía lo distribuye (más resiliente y desacoplada, pero más difícil de razonar como un todo — por eso la trazabilidad vía Prefect, taggeada por `sagaId`, es tan importante en este modelo).
