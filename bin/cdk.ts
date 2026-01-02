#!/usr/bin/env node
import "source-map-support/register";
import buildConfig from "../config/config";
import Name from "../helpers/names";
import { App, Tags } from "aws-cdk-lib";
import PipelineStack from "../lib/pipeline-stack";

const app = new App();

const name = new Name({
  parentOrg: buildConfig.Tags.ParentOrg,
  department: buildConfig.Tags.Department,
  app: buildConfig.Tags.App,
  environment: buildConfig.Tags.Environment, 
  processType: buildConfig.Tags.ProcessType,
  project: buildConfig.Tags.Project,
});

for (const [key, value] of Object.entries(buildConfig.Tags)) {
  if (value) {
    Tags.of(app).add(key, value);
  }
}

new PipelineStack(app, name.createResource("pipeline-stack"), {
  // Configuración del entorno de Tooling/Despliegue (donde vive el pipeline)
  env: {
    account: buildConfig.DeploymentAccount,
    region: buildConfig.DeploymentRegion,
  },
  // Inyectamos las dependencias
  buildConfig: buildConfig,
  name: name,
});

app.synth();