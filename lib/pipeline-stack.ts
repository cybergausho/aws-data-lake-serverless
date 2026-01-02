import { Stack } from "aws-cdk-lib";
import {
  BuildSpec,
  LinuxBuildImage,
  PipelineProject,
} from "aws-cdk-lib/aws-codebuild";
import { Artifact, Pipeline } from "aws-cdk-lib/aws-codepipeline";
import {
  CodeBuildAction,
  CodeStarConnectionsSourceAction,
} from "aws-cdk-lib/aws-codepipeline-actions";
import {
  CodePipeline,
  CodePipelineFileSet,
  ManualApprovalStep,
  ShellStep,
} from "aws-cdk-lib/pipelines";
import StageStack from "./stage-stack";
import { Construct } from "constructs";
import PipelineStackProps from "../helpers/interfaces/pipeline-stack-props-interface";

export default class PipelineStack extends Stack {
  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);

    const { buildConfig, name } = props;

    const sourceOutput = new Artifact(name.createResource("source-output"));

    const sourceAction = new CodeStarConnectionsSourceAction({
      actionName: "GitLab_Source",
      owner: buildConfig.GitLabSettings.Owner, // Usuario - Grupo
      repo: buildConfig.GitLabSettings.Repo,   // Nombre del proyecto
      branch: buildConfig.GitLabSettings.Branch, // Branch
      connectionArn: buildConfig.GitLabSettings.ConnectionArn, // ARN
      output: sourceOutput,
      triggerOnPush: true,
    });


    const testProject = new PipelineProject(this, "TestBuildProject", {
      projectName: name.createResource("build-test"),
      environment: {
        buildImage: LinuxBuildImage.STANDARD_7_0,
      },
      buildSpec: BuildSpec.fromObject({
        version: "0.2",
        phases: {
          install: {
            "runtime-versions": {
              nodejs: "20", // Node.js 20
              python: "3.12", // Python 3.12
              ruby: "3.3", // Ruby 3.3
            },
            commands: [
              "npm ci",
              "npm install -g aws-cdk",
              "gem install cfn-nag",
              "python -m venv .ve",
              ". .ve/bin/activate",
              "pip install cfn-lint",
              "pip install semgrep",
            ],
          },
          build: {
            commands: [
              "npm run test",
              "npx cdk synth",
              "mkdir ./cfnnag_output",
              `for template in $(find ./cdk.out -maxdepth 2 -type f -name '*.template.json'); do cp $template ./cfnnag_output; done`,
              "cfn_nag_scan -v",
              "cfn_nag_scan --deny-list-path .cfnnagignore --input-path ./cfnnag_output",
              "cfn-lint ./cfnnag_output/*",
              "semgrep --config=p/javascript ./cfnnag_output",
            ],
          },
        },
      }),
    });

    const testOutput = new Artifact(name.createResource("build-output"));

    const testAction = new CodeBuildAction({
      actionName: "build-test",
      project: testProject,
      input: sourceOutput,
      outputs: [testOutput],
    });

    const pipeline = new Pipeline(this, "Pipeline", {
      pipelineName: name.createResource("pipeline"),
      enableKeyRotation: true,
      crossAccountKeys: true,
      stages: [
        {
          actions: [sourceAction],
          stageName: "Source",
        },
        {
          actions: [testAction],
          stageName: "Test",
        },
      ],
    });

    const buildFileSet = CodePipelineFileSet.fromArtifact(sourceOutput);

    const cdkPipeline = new CodePipeline(this, "CdkPipeline", {
      selfMutation: true,
      codePipeline: pipeline,
      synth: new ShellStep("Synth", {
        input: buildFileSet,
        commands: [
          "npm ci",
          "npx cdk synth",
        ],
      }),
    });

    //Crea un stage por cada environment que lee del Buildconfig(YAML)
    for (const [key, value] of Object.entries(buildConfig.Environments)) {
      name.environment = value.Name;
      const pipelineStage = new StageStack(this, value.Name, {
        env: {
          account: value.Account,
          region: value.Region,
        },
        params: value,
        stackName: name.createResource("deployment-stack"),
      });

      //Añade a la pipeline el stage del environment que lee arriba. Termina y vuelve a comenzar el ciclo.
      if (value.ManualApprovalStep) {
        cdkPipeline.addStage(pipelineStage, {
          pre: [new ManualApprovalStep(value.ManualApprovalStep)],
        });
      } else {
        cdkPipeline.addStage(pipelineStage);
      }
    }
  }
}
