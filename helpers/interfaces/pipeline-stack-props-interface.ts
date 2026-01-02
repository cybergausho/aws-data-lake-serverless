import { StackProps } from "aws-cdk-lib";
import Name from "../names";
import BuildConfig from "./build-config-interface";

export default interface PipelineStackProps extends StackProps {
  env: Environment;
  buildConfig: BuildConfig;
  name: Name;
}

interface Environment {
  account: string;
  region: string;
}
