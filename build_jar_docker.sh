#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-$HOME/practica_creativa}"
SBT_LAUNCHER="$PROJECT_HOME/.tools/sbt"

if ! command -v java >/dev/null 2>&1; then
  echo "Java no esta instalado o no esta en PATH. Instala Java 17 antes de compilar."
  exit 1
fi

mkdir -p "$PROJECT_HOME/.tools"

if [ ! -x "$SBT_LAUNCHER" ]; then
  curl -fsSL https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt -o "$SBT_LAUNCHER"
  chmod +x "$SBT_LAUNCHER"
fi

cd "$PROJECT_HOME/flight_prediction"
"$SBT_LAUNCHER" assembly

echo "JAR generado: $PROJECT_HOME/flight_prediction/target/scala-2.13/flight_prediction_2.13-0.1.jar"