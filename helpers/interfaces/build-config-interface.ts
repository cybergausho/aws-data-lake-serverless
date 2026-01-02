export default interface BuildConfig {
  DeploymentAccount: string;
  DeploymentRegion: string;
  GitLabSettings: {
    Owner: string;
    Repo: string;
    Branch: string;
    ConnectionArn: string;
  };
  Tags: TagsConfig;
  Environments: EnvironmentsConfig;
}

interface TagsConfig {
  ParentOrg: string;
  Department: string;
  App: string;
  Environment: string | undefined;
  Version: string;
  ProcessType: string;
  Project: string;
}

interface EnvironmentsConfig {
  [key: string]: EnvironmentConfig;
}

export interface EnvironmentConfig {
  Name: string;
  Account: string;
  Region: string;
  ProjectName: string;
  ManualApprovalStep?: string;
}
