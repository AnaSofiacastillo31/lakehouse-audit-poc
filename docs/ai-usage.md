# C. Uso orquestado de IA

**Asistente:** Claude (Anthropic), en una sola conversación de trabajo. Se usó como par técnico para analizar el enunciado y generar el código (PySpark, dbt, Docker Compose)

**Cómo leer este documento.** Separo honestamente tres cosas distintas:
1. **Errores de la IA detectados por la ejecución real o por mí**: las correcciones mias propiamente dichas.
2. **Auto-correcciones de la IA**: la IA cambió su propia propuesta sin que yo interviniera. No las presento como mérito mío.
3. **Decisiones de diseño que yo asumo y defiendo**, aunque la IA las haya redactado.

---

## Prompts clave

| # | Prompt (resumido) | Para qué sirvió |
|---|---|---|
| 1 | trace los requisitos de entrega "analiza la información y tracemos una línea para empezar". | Plan por fases y la comparación Outbox / CDC / buffer. |
| 2 |  Corregir que la IA estaba especulando sobre partes del enunciado que no tenía. |
| 3 | Errores reales pegados tal cual: instalador de Docker, `wsl --install` → 403, `pull access denied for minio/mc`, timeout contra `mcr.microsoft.com`. | Diagnóstico y corrección guiados por evidencia, no por suposiciones.
| 5 | Validar los resultados y documentar los límites encontrados (silver vs. bronze, `remove_orphan_files`). |

---

## 1. Errores de la IA corregidos gracias a la ejecución real

### 1.1 MinIO: una imagen que ya no existía (el caso principal)
- **Qué hizo la IA:** usó `minio/minio` y `minio/mc` como S3 local y lo entregó como listo para usar.
- **Qué pasó:** en la primera ejecución, `docker compose up` falló con `pull access denied for minio/mc`. MinIO había retirado sus imágenes públicas de Docker Hub y de quay.io. El conocimiento de la IA era anterior a ese cambio, y no lo verificó aunque podía hacerlo.
- **Corrección:** se reemplazó por SeaweedFS (API S3) y el bucket se crea con la aws-cli oficial. Antes de volver a entregar, se comprobó en Docker Hub que existiera cada imagen nueva.
- **Quién aportó qué:** el error lo destapó ejecutar el entorno de verdad. La alternativa (SeaweedFS) la propuso la IA. Mi criterio fue exigir ejecución real antes de dar algo por terminado y descartar mirrors no oficiales de MinIO.

### 1.2 `wsl --install` → 403
- **Qué hizo la IA:** dio el paso estándar, `wsl --install`.
- **Qué pasó:** en este equipo devolvió `Prohibido (403)`.
- **Corrección:** activar `VirtualMachinePlatform` y `Microsoft-Windows-Subsystem-Linux` con `dism.exe`, y luego `wsl --update --web-download`.

### 1.3 `remove_orphan_files` presentado como parte de la solución
- **Qué hizo la IA:** lo incluyó en el mantenimiento y en el README como si funcionara, envuelto en un `try/except` sin advertir que probablemente fallaría.
- **Qué pasó:** en la ejecución real falló. Iceberg 1.6 lista archivos con el FileSystem de Hadoop, que no conoce `s3://` sin `hadoop-aws`.
- **Corrección:** se documentó como límite explícito. En AWS lo cubre el optimizador de huérfanos del Glue Data Catalog.

### 1.4 Descargas desde `mcr.microsoft.com`
- **Qué pasó:** la compilación falló por timeout de red contra el registro de Microsoft.
- **Corrección:** pre-descargar las imágenes con `docker pull`, que reintenta, y usar la misma imagen base (`aspnet:8.0`) para la API y el relay, para tener una descarga menos.

### 1.5 Especular sin información
- Al inicio, la IA pedía y daba por supuestas preguntas del enunciado que todavía no le había entregado. Le indiqué que trabajara solo con lo que tenía.

---

## 2. Auto-correcciones de la IA (no son mérito humano)

En esta parte no intervine. Las dejo registradas por transparencia:

- **Imagen de Spark:** la IA propuso primero `tabulario/spark-iceberg` y luego la cambió por una imagen propia sobre `apache/spark:3.5.3`, con jars fijadas y alineadas con Glue.
- **dbt:** fijó dbt 1.9. Al validar con `dbt parse` vio el aviso de versión sin soporte y subió a 1.11. También corrigió la sintaxis de tests deprecada.
- **dbt sin Thrift Server:** cambió su propia propuesta inicial (dbt contra un Thrift Server) por el modo `session`, para ahorrar RAM.
- **Prueba de la lógica:** antes de que yo ejecutara nada, la IA probó las transformaciones de bronze, silver y gold en Spark local con datos sintéticos que incluían un duplicado y un mensaje corrupto.

> Una versión anterior de este documento presentaba varios de estos puntos (y el tema de Glue) como "correcciones humanas". No lo eran: fueron decisiones o auto-revisiones de la IA. Lo corregí porque presentar como propio un criterio que no ejercí sería justamente el tipo de output de IA que hay que controlar.

---

## 3. Decisiones de diseño que asumo y defiendo

La IA las redactó, pero las revisé y son las que voy a sostener en la sesión técnica:

- **Outbox en lugar de CDC o buffer:** el requisito "sin pérdida de eventos" descarta el buffer en memoria, y el acoplamiento al esquema físico hace de CDC una segunda opción.
- **AWS Glue en local:** el Glue Data Catalog no tiene emulador local oficial. Se usa un catálogo Iceberg REST y en AWS solo cambia `catalog-impl`.
- **At-least-once honesto:** el relay entrega at-least-once y silver deduplica por `event_id`, en lugar de prometer exactly-once de punta a punta.
- **Particionamiento** por `days(occurred_at)` y no por cuenta, **copy-on-write** para auditoría, y **purga por lotes de menos de 5000 filas** para evitar el escalamiento de bloqueos.
- **Riesgo conocido:** en la PoC la purga del outbox no espera a confirmar que el evento llegó a bronze (ver la pregunta 3 del README).

<!-- TODO (autora): reemplazar o complementar esta sección con tus propias palabras: qué revisaste, qué cambiarías y por qué. -->

---

## 4. Lección

La IA aceleró muchísimo la escritura de código y documentación. Sus fallas no fueron de sintaxis sino de **contexto del mundo real**: imágenes retiradas, redes restringidas, limitaciones de una versión concreta. Ninguna se ve leyendo el código. Todas aparecieron al ejecutar. Por eso mi regla es simple: **nada sugerido por la IA cuenta como hecho hasta que corre en la máquina real**.
