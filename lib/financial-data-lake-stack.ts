import { Stack, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as fs from "fs";
import * as path from "path";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { GlueEtlConstruct } from "./constructs/glue-etl";
import CustomStackProps from "../helpers/interfaces/custom-stack-props-interface";

export class FinancialDataLakeStack extends Stack {
  constructor(scope: Construct, id: string, props: CustomStackProps) {
    super(scope, id, props);

    // DATA LAYER: Creamos los Buckets
    // ---------------------------------------------------------
    const rawBucket = new s3.Bucket(this, "RawBucket", {
      bucketName: `fin-datalake-raw-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY,
      lifecycleRules: [{
        expiration: Duration.days(30), // limpia la basura en 30 dias
      }]
    });

    const assetsBucket = new s3.Bucket(this, "AssetsBucket", {
      bucketName: `fin-datalake-assets-${this.account}`,
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // IAM & SECURITY: Roles con Least Privilege
    // ---------------------------------------------------------
    const glueRole = new iam.Role(this, "GlueJobRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
    });
    
    rawBucket.grantReadWrite(glueRole);
    assetsBucket.grantRead(glueRole);
    // Agregamos la policy necesaria para logs
    glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"));


    // DEPENDENCIES LAYER to Lambda
    const dependenciesLayer = new lambda.LayerVersion(this, 'RequestLayer', {
      code: lambda.Code.fromAsset('lambda/request-layer.zip'), 
      
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
      description: 'Librerias externas de lambda (requests)',
      layerVersionName: `ingestion-layer-${this.account}`
    });

    
    // INGESTION LAYER: Lambda
    // ---------------------------------------------------------
    const fetchLambda = new lambda.Function(this, "IngestLambda", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: "drupal_raw_to_s3.lambda_handler",
      code: lambda.Code.fromAsset("lambda"),
      timeout: Duration.minutes(5),
      layers: [dependenciesLayer],
      environment: {
        RAW_BUCKET: rawBucket.bucketName,
        API_URL: "https://buenosaires.gob.ar/api/consulta-tramites-obelisco-rest",
      },
    });

    // Permisos automaticos
    rawBucket.grantWrite(fetchLambda);

    // TRANSFORMATION LAYER: Glue Jobs
    // ---------------------------------------------------------
    
    // deploy de scripts
    new s3deploy.BucketDeployment(this, "DeployScripts", {
      sources: [s3deploy.Source.asset("./glue-assets/scripts")],
      destinationBucket: assetsBucket,
      destinationKeyPrefix: "scripts/",
    });

    // nombres de los glue job 
    const jobName1 = "drupal-raw-to-staging";
    const jobName2 = "drupal-staging-to-consume";

    // instanciamos nuevo objeto 
    new GlueEtlConstruct(this, "DrupalEtlJobs", {
      bucketAssets: assetsBucket,
      bucketRaw: rawBucket,
      role: glueRole,
      scriptPath: "./glue-assets/scripts",
      jobNames: [jobName1, jobName2],
    });
  
    // ORCHESTRATION LAYER: Step Functions
    // ---------------------------------------------------------

    // rol para Step Function
    const sfnRole = new iam.Role(this, "StepFunctionRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
    });
    
    // permiso explicito para ejecutar los jobs declarados
    sfnRole.addToPolicy(new iam.PolicyStatement({
        actions: ["glue:StartJobRun", "glue:GetJobRun", "glue:BatchStopJobRun"],
        resources: [
            `arn:aws:glue:${this.region}:${this.account}:job/${jobName1}`,
            `arn:aws:glue:${this.region}:${this.account}:job/${jobName2}`
        ],
    }));

    // lectura e inyeccion de variables
    const definitionPath = path.join(__dirname, "../step-functions/drupal_pipeline.json");
    let definitionBody = fs.readFileSync(definitionPath, "utf8");

    // reemplazo variables dinamicas
    definitionBody = definitionBody.replace('${JOB_RAW_TO_STAGING}', jobName1);
    definitionBody = definitionBody.replace('${JOB_STAGING_TO_CONSUME}', jobName2);

    // creo de la State Machine
    new sfn.CfnStateMachine(this, "DrupalPipeline", {
      stateMachineName: "drupal_pipeline",
      roleArn: sfnRole.roleArn,
      definitionString: definitionBody,
    });
  
  }
  }