# Financial Data Lake - Guía de Setup

Instrucciones paso a paso para desplegar el proyecto en una cuenta AWS nueva.

## 📋 Prerrequisitos
- Node.js v20+
- Python 3.11+
- AWS CLI configurado
- AWS CDK Toolkit instalado globalmente (`npm install -g aws-cdk`)

## 🛠️ Estructura del Proyecto
```text
/
├── bin/cdk.ts             # Punto de entrada de la App
├── config/config.yaml     # CONFIGURACIÓN CENTRAL
├── glue-assets/scripts/   # Scripts ETL (Python)
├── lambda/                # Código de Ingesta + Layers
├── lib/
│   ├── pipeline-stack.ts  # Definición del CI/CD
│   ├── stage-stack.ts     # Conector de ambientes
│   └── financial-stack.ts # Infraestructura (S3, Glue, etc)
└── step-functions/        # Definición JSON de la máquina de estados
```


## Guía de Configuración y Despliegue

Sigue estos pasos para configurar, preparar y desplegar el proyecto Financial Data Lake.

---

## 1. Configuración (Paso Crítico)

Antes de desplegar, debes editar el archivo `config/config.yaml` con tus datos reales.

### A. Conectar GitLab con AWS
1. Ve a la consola de AWS -> **Developer Tools** -> **Settings** -> **Connections**.
2. Crea una conexión a **GitLab** y autorízala.
3. Copia el **ARN** de la conexión resultante.

### B. Editar `config/config.yaml`
Modifica las siguientes secciones en el archivo de configuración:

```yaml
# Cuenta donde vive el Pipeline (Tooling Account)
DeploymentAccount: "123456789012"  # <-- TU CUENTA PRINCIPAL
DeploymentRegion: "us-east-1"

GitLabSettings:
  Owner: "tu-usuario-gitlab"       # <-- TU USUARIO/GRUPO
  Repo: "financial-data-lake"      # <-- TU REPO
  Branch: "main"
  ConnectionArn: "arn:aws:codestar..." # <-- EL ARN DEL PASO A

Environments:
  Development:
    Name: "Development"
    Account: "123456789012"        # <-- CUENTA PARA DEV
    Region: "us-east-1"
    ProjectName: "fin-lake-dev"

  Production:
    Name: "Production"
    Account: "123456789012"        # <-- CUENTA PARA PROD
    Region: "us-east-1"
    ProjectName: "fin-lake-prod"
    ManualApprovalStep: "DeployToProd"
```

## 2. Preparar Dependencias
### Instalar paquetes Node
```bash
npm install
```

### Verificar Lambda Layer
Asegúrate de que existe el archivo lambda/request-layer.zip

## 3. Despliegue

Para preparar el código:
Correr el test:
```bash
npm test -- -u
```
Transformar la plantilla:
```bash
cdk synth
```
Bootstrap (Solo la primera vez)

Debes preparar tu cuenta AWS para CDK Pipelines. Ejecuta esto reemplazando tu cuenta y región:

```bash
npx cdk bootstrap aws://TU_CUENTA/us-east-1 --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess
```

## Despliegue del Pipeline

Este comando crea el Pipeline. Una vez creado, el pipeline se encargará de desplegar la infraestructura de Dev y Prod automáticamente.

```bash
cdk deploy
```

## 🔄 Flujo de Trabajo

- Haz cambios en tu código local.
- Haz git push a la rama main en GitLab.
-  AWS CodePipeline detectará el cambio automáticamente:
- Ejecutará tests de seguridad.
- Desplegará en Development.
- Esperará aprobación manual para Production.