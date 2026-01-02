import fs from "fs";
import path from "path";
import yaml from "yaml";
import BuildConfig from "../../helpers/interfaces/build-config-interface";

const configFile = fs.readFileSync(
  path.resolve("./config/config.yaml"),
  "utf-8"
);

const buildConfig: BuildConfig = yaml.parse(configFile);

export default buildConfig;
