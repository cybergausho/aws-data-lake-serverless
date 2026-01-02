import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

interface GlueEtlProps {
  bucketAssets: s3.IBucket;
  bucketRaw: s3.IBucket;
  role: iam.IRole;
  scriptPath: string;
  jobNames: string[];
}

export class GlueEtlConstruct extends Construct {
  constructor(scope: Construct, id: string, props: GlueEtlProps) {
    super(scope, id);

    props.jobNames.forEach((name) => {
      new glue.CfnJob(this, `Job-${name}`, {
        name: name,
        role: props.role.roleArn,
        command: {
          name: 'glueetl',
          pythonVersion: '3',
          scriptLocation: `s3://${props.bucketAssets.bucketName}/scripts/${name}.py`,
        },
        defaultArguments: {
          '--bucketRaw': props.bucketRaw.bucketName,
          '--enable-metrics': 'true',
          '--enable-continuous-cloudwatch-log': 'true',
          '--job-bookmark-option': 'job-bookmark-enable', // marcador de lectura
        },
        glueVersion: '4.0',
        workerType: 'G.1X',
        numberOfWorkers: 2,
      });
    });
  }
}