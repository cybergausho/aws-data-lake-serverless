interface NameConfig {
  parentOrg: string;
  department: string;
  app: string;
  environment: string | undefined;
  processType: string;
  project: string;
}

export default class Name {
  public readonly parentOrg: string;
  public readonly department: string;
  public readonly app: string;
  public environment: string | undefined;
  public readonly processType: string;
  public readonly project: string;

  constructor(config: NameConfig) {
    this.parentOrg = config.parentOrg;
    this.department = config.department;
    this.app = config.app;
    this.processType = config.processType;
    this.project = config.project;
    this.environment = config.environment;
  }

  public createResource(resourceType: string): string {
    const tags = [
      this.parentOrg,
      this.department,
      this.app,
      this.environment,
      resourceType,
    ].filter(Boolean);

    return tags.join("-").toLowerCase();
  }

  public createParameter(resourceType: string): string {
    const tags = [
      this.parentOrg,
      this.department,
      this.app,
      "ssm",
      this.environment,
      resourceType,
    ].filter(Boolean);

    return "/" + tags.join("/").toLowerCase();
  }
}
