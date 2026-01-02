import { Stage, Tags } from "aws-cdk-lib";
import buildConfig from "../config/config";
import { FinancialDataLakeStack } from "./financial-data-lake-stack";
import CustomStackProps from "../helpers/interfaces/custom-stack-props-interface";
import { Construct } from "constructs";

export default class StageStack extends Stage {
  constructor(scope: Construct, id: string, props: CustomStackProps) {
    super(scope, id, props);

    const { env, params, stackName } = props;

    const stack = new FinancialDataLakeStack(this, "AppStackStage", {
      env,
      params,
      stackName,
    });

    for (const [key, value] of Object.entries(buildConfig.Tags)) {
      Tags.of(stack).add(key, value);
    }
  }
}
