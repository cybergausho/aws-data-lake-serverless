import { Stack } from "aws-cdk-lib";
import Name from "../helpers/names";
import buildConfig from "./config-test/config-test";
import PipelineStack from "../lib/pipeline-stack";
import { Template } from "aws-cdk-lib/assertions";

describe("Standard PipeLine", () => {
  test("Creates a Standard PipeLine", () => {
    const app = new Stack();

    const name = new Name({
      parentOrg: buildConfig.Tags.ParentOrg,
      department: buildConfig.Tags.Department,
      app: buildConfig.Tags.App,
      environment: buildConfig.Tags.Environment,
      processType: buildConfig.Tags.ProcessType,
      project: buildConfig.Tags.Project,
    });

    const cdkPipelineStack = new PipelineStack(app, "AppStack", {
      env: {
        account: buildConfig.DeploymentAccount,
        region: buildConfig.DeploymentRegion,
      },
      buildConfig,
      name,
    });

    const template = Template.fromStack(cdkPipelineStack);

    expect(template.toJSON()).toMatchSnapshot("AppStack");
  });
});
