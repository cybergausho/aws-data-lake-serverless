import { StackProps } from "aws-cdk-lib";
import Name from "../names";
import { EnvironmentConfig } from "./build-config-interface";

export default interface CustomStackProps extends StackProps {
  env: Environment;
  params: EnvironmentConfig;
  stackName: string;
}

interface Environment {
  account: string;
  region: string;
}
