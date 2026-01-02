# Data Lake - Arquitectura Técnica

Este proyecto implementa un Data Lake Serverless en AWS, diseñado para ser escalable, seguro y gestionado completamente mediante Infraestructura como Código (IaC) con AWS CDK y GitLab como control de versiones.

## 1. Visión General
El objetivo es ingerir datos mediante una api de lambda, almacenarlos en crudo y procesarlos mediante transformaciones ETL orquestadas para su consumo.

### Diagrama de Flujo
1. **Ingesta:** Lambda conecta a API publica externas -> Guarda JSON en S3 Raw.
2. **Orquestación:** Step Functions activa el flujo ETL.
3. **Procesamiento:** AWS Glue toma la data cruda -> Transforma -> Guarda en Staging/Consume.
4. **Gobierno:** Todo el ciclo de vida gestionado por CDK Pipelines.

---

## 2. Capas de la Arquitectura

### 🔹 Data Layer (Almacenamiento)
- **Raw Bucket:** Almacena la data tal cual llega de la fuente.
  - *Seguridad:* Encriptación S3 Managed, Bloqueo de acceso público.
  - *Ciclo de vida:* Eliminación automática de objetos tras 30 días (Cost Optimization).
- **Assets Bucket:** Almacena scripts de Glue (`.py`) y artefactos de despliegue.

### 🔹 Ingestion Layer
- **Lambda Function (`IngestLambda`):**
  - Runtime: Python 3.11.
  - **Lambda Layer:** Incorpora librerías externas (`requests`, etc.) mediante un ZIP gestionado.
  - **Variables de Entorno:** Configuración dinámica de endpoints y buckets.

### 🔹 Transformation Layer (ETL)
- **AWS Glue Jobs:**
  - `drupal-raw-to-staging`: Limpieza inicial.
  - `drupal-staging-to-consume`: Agregación final.
  - **Modularidad:** Implementado vía `GlueEtlConstruct` para reutilización de código de infraestructura.

### 🔹 Orchestration Layer (Control)
- **AWS Step Functions:**
  - Máquina de Estados que coordina la ejecución secuencial de los Jobs de Glue.
  - Manejo de reintentos (Retries) y control de errores nativo.

---

## 3. Estrategia de DevOps (CI/CD)

### CDK Pipelines
Utilizamos un pipeline "Self-Mutating" (que se actualiza a sí mismo) basado en CodePipeline.

- **Fuente:** Conexión a GitLab (vía AWS CodeStar Connections).
- **Seguridad (DevSecOps):**
  - Escaneo de infraestructura con `cfn_nag`, `cfn-lint` y `semgrep` antes del despliegue.
- **Multi-Environment:**
  - Despliegue automático en `Development`.
  - Aprobación manual requerida para `Production`.
- **Configuration Driven:**
  - Toda la configuración de entornos y tags se inyecta desde `config/config.yaml`.

---

## 4. Decisiones de Diseño Clave
- **Least Privilege:** Roles IAM granulares para cada servicio (Lambda solo escribe en Raw, Glue solo lee lo necesario).
- **Clean Architecture:** Separación estricta entre Lógica (Python), Infraestructura (CDK Stacks) y Configuración (YAML).