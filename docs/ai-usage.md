# C. Uso orquestado de IA

**Asistente:** Claude (Anthropic), usado como par técnico durante toda la prueba: análisis del enunciado, preparación del entorno, generación de código y documentación.

**Regla de trabajo:** la IA propone y el humano decide, valida y corrige. Abajo están los prompts clave y, sobre todo, los puntos donde la respuesta de la IA fue genérica o incorrecta y hubo que intervenir.

---

## 1. Análisis del enunciado y plan

**Prompt clave**
> Se pegó el contexto de negocio, el reto (A–D) y los requisitos de entrega: "analiza la información y tracemos una línea para empezar".

**Aporte de la IA:** comparó Outbox, CDC y buffer asíncrono, propuso capas bronze/silver/gold en Iceberg y un plan en seis fases.

**Corrección humana**
- La IA empezó a pedir y suponer contenido del enunciado que no se le había entregado. Se le indicó que trabajara solo con la información disponible.
- La IA ofreció Outbox, CDC y buffer como opciones equivalentes. La elección se hizo con el requisito literal "sin pérdida de eventos", que descarta el buffer en memoria, y con el criterio de bajo acoplamiento al esquema físico, que descarta CDC como primera opción.

## 2. Preparación del entorno (Windows, 16 GB de RAM)

**Problemas reales**
- El instalador de Docker Desktop falló porque faltaba *Virtual Machine Platform*.
- `wsl --install` devolvió **HTTP 403**.

**Respuesta genérica de la IA:** "ejecuta `wsl --install`". Eso no funcionó en este equipo.

**Solución aplicada:**
1. Activar las características con `dism.exe` (`VirtualMachinePlatform`, `Microsoft-Windows-Subsystem-Linux`).
2. Instalar el motor con `wsl --update --web-download`.
3. Limitar WSL a 10 GB con `.wslconfig`.

El presupuesto de 16 GB de RAM condicionó todo el diseño del compose: límites de memoria por servicio y dbt sin Thrift Server.

## 3. AWS Glue en local

**Riesgo típico de la IA:** sugerir que "Glue se levanta en Docker" o usar LocalStack. El **Glue Data Catalog no tiene un emulador local oficial**, y en LocalStack es una función de pago.

**Decisión humana:** usar un catálogo Iceberg REST como sustituto y documentar que en AWS solo cambia `catalog-impl`. Se eligieron versiones de Spark e Iceberg alineadas con Glue 4.0/5.0 para que los jobs sean portables.

## 4. Imagen de Spark

**Primera propuesta de la IA:** la imagen de terceros `tabulario/spark-iceberg`, la del quickstart de Iceberg.

**Decisión:** construir una imagen propia sobre `apache/spark:3.5.3` con las jars exactas (Iceberg 1.6.1, AWS bundle y conector Kafka). Razones:
- control de versiones
- paridad con Glue
- evitar procesos innecesarios (Jupyter, Thrift, History Server) que consumen RAM

## 5. dbt

- La IA fijó **dbt 1.9**. Al validar el proyecto, dbt avisó que esa versión ya no recibe parches, y se subió a **dbt-core/dbt-spark 1.11**.
- La sintaxis de tests generada (`accepted_values` con argumentos sueltos) disparaba una advertencia de deprecación en 1.11. Se corrigió a `data_tests` con `arguments:`.
- La IA propuso dbt contra un Spark Thrift Server. Se cambió al modo `session`, que no necesita un servicio permanente y ahorra unos 2 GB de RAM.

## 6. Verificaciones hechas (sin confiar solo en la IA)

- `docker compose config` para validar la sintaxis del compose.
- `dbt parse` y `dbt ls`: el proyecto compila (2 modelos, 6 tests, 1 source).
- La lógica de bronze (parseo del envelope, cuarentena de mensajes malformados) y de silver/gold (deduplicación por `event_id`, coexistencia v1/v2) se probó en Spark local con datos sintéticos que incluían un duplicado y un mensaje corrupto.
- La compilación .NET y el flujo completo se validan con `docker compose up --build` en la máquina local.

## 7. Puntos donde se exigió precisión técnica

- **Lock escalation:** la purga se hace en lotes de **menos de 5000 filas**, el umbral de escalamiento de bloqueos de SQL Server.
- **Particionamiento:** se rechazó particionar por cuenta (alta cardinalidad, más archivos pequeños). Se usó `days(occurred_at)` con hidden partitioning.
- **Copy-on-write vs. merge-on-read:** para auditoría (append-only, borrados raros) se eligió COW para priorizar la lectura.
- **Semántica de entrega:** se documentó honestamente como at-least-once en el relay más deduplicación en silver, en lugar de prometer exactly-once de punta a punta.

---

> **Nota para quien revisa:** las decisiones de las secciones 1, 3 y 7 son las que defenderé en la sesión. La IA aceleró la escritura de código y documentación; los criterios de elección y la validación fueron humanos.
