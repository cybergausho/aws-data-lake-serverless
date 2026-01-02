// config/config.ts (O donde tengas este archivo lector)
import fs from "fs";
import path from "path";
import yaml from "yaml";
import BuildConfig from "../helpers/interfaces/build-config-interface";

// __dirname para que siempre encuentre el archivo sin importar donde se ejecute
const configPath = path.join(__dirname, "config.yaml"); 

// Verificacion de seguridad para debug
if (!fs.existsSync(configPath)) {
  throw new Error(`No se encontró el archivo de configuración en: ${configPath}`);
}

const configFile = fs.readFileSync(configPath, {
  encoding: "utf-8",
});

const buildConfig: BuildConfig = yaml.parse(configFile);

export default buildConfig;