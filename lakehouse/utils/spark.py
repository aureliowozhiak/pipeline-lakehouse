"""Criação da SparkSession com Delta Lake, robusta para Windows, Linux e macOS.

Windows sem winutils.exe
------------------------
O Hadoop usado pelo Spark chama winutils.exe para aplicar permissões de arquivo.
Quando HADOOP_HOME/bin/winutils.exe não existe, ativamos um "modo compatível":

* HADOOP_HOME aponta para uma pasta vazia em .cache/hadoop (o Hadoop só exige que exista);
* o sistema de arquivos local é trocado por br.lakehouse.hadoop.BareLocalFileSystem
  (tools/windows), que ignora chmod/chown — sem efeito prático no Windows;
* os JARs do Delta são colocados no classpath do driver (em vez de spark.jars.packages,
  cuja distribuição de arquivos também exige winutils). Eles são baixados uma única vez
  do Maven Central pelo Ivy que já vem no PySpark — o mesmo que
  configure_spark_with_delta_pip faria.

Se o winutils estiver instalado (HADOOP_HOME configurado), usamos o caminho padrão
com configure_spark_with_delta_pip.
"""

from __future__ import annotations

import importlib.metadata
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from lakehouse import PROJECT_ROOT
from lakehouse.config.settings import Settings
from lakehouse.utils.java_env import IS_WINDOWS, JavaInfo, ensure_java
from lakehouse.utils.logging_utils import get_logger

log = get_logger("spark")

CACHE_DIR = PROJECT_ROOT / ".cache"
WINDOWS_TOOLS = PROJECT_ROOT / "tools" / "windows"
BARE_FS_JAR = WINDOWS_TOOLS / "bare-local-fs.jar"
BARE_FS_CLASS = "br.lakehouse.hadoop.BareLocalFileSystem"


def _pyspark_jars_dir() -> Path:
    import pyspark

    return Path(pyspark.__file__).parent / "jars"


def _scala_binary_version() -> str:
    for jar in _pyspark_jars_dir().glob("scala-library-*.jar"):
        version = jar.stem.replace("scala-library-", "")
        return ".".join(version.split(".")[:2])
    return "2.12"


def winutils_available() -> bool:
    home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    return bool(home) and (Path(home) / "bin" / "winutils.exe").exists()


def _run_quiet(cmd: list[str], what: str) -> None:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-15:])
        raise RuntimeError(f"Falha ao {what}:\n{tail}")


def ensure_delta_jars(java: JavaInfo) -> list[Path]:
    """JARs do Delta Lake (delta-spark + dependências) em .cache/jars/delta-<versão>."""
    delta_version = importlib.metadata.version("delta-spark")
    scala = _scala_binary_version()
    target = CACHE_DIR / "jars" / f"delta-{delta_version}_{scala}"
    main_jar_name = f"io.delta_delta-spark_{scala}-{delta_version}.jar"
    if (target / main_jar_name).exists():
        return sorted(target.glob("*.jar"))

    target.mkdir(parents=True, exist_ok=True)
    ivy_jars = Path.home() / ".ivy2" / "jars"
    needed = [main_jar_name, f"io.delta_delta-storage-{delta_version}.jar"]
    if all((ivy_jars / n).exists() for n in needed) and list(ivy_jars.glob("org.antlr_antlr4-runtime-*.jar")):
        for jar in needed + [p.name for p in ivy_jars.glob("org.antlr_antlr4-runtime-*.jar")]:
            shutil.copy2(ivy_jars / jar, target / jar)
        log.info("JARs do Delta Lake copiados do cache do Ivy (~/.ivy2/jars).")
        return sorted(target.glob("*.jar"))

    ivy_jar = next(_pyspark_jars_dir().glob("ivy-*.jar"), None)
    if ivy_jar is None:
        raise RuntimeError("ivy-*.jar não encontrado na instalação do PySpark.")
    log.info("Baixando JARs do Delta Lake %s do Maven Central (somente na primeira execução)...", delta_version)
    _run_quiet(
        [
            java.executable, "-jar", str(ivy_jar),
            "-dependency", "io.delta", f"delta-spark_{scala}", delta_version,
            "-confs", "default", "-types", "jar",
            "-cache", str(CACHE_DIR / "ivy"),
            "-retrieve", str(target).replace("\\", "/") + "/[organisation]_[artifact]-[revision](-[classifier]).[ext]",
        ],
        "baixar os JARs do Delta Lake (é necessária internet apenas na primeira execução)",
    )
    for jar in target.glob("org.scala-lang_*.jar"):
        jar.unlink()  # o Spark já traz a biblioteca Scala
    if not (target / main_jar_name).exists():
        raise RuntimeError(f"Download dos JARs do Delta incompleto em {target}")
    return sorted(target.glob("*.jar"))


def ensure_bare_fs_jar(java: JavaInfo) -> Path:
    """Compila tools/windows/bare-local-fs.jar se ainda não existir (requer javac / JDK)."""
    if BARE_FS_JAR.exists():
        return BARE_FS_JAR
    javac = None
    if java.home:
        candidate = Path(java.home) / "bin" / "javac.exe"
        javac = str(candidate) if candidate.exists() else None
    javac = javac or shutil.which("javac")
    if not javac:
        raise RuntimeError(
            f"{BARE_FS_JAR} não existe e não há 'javac' (JDK) para compilá-lo. "
            "Instale um JDK (não apenas JRE) ou configure o winutils.exe (veja o README)."
        )
    hadoop_api = next(_pyspark_jars_dir().glob("hadoop-client-api-*.jar"))
    build_dir = WINDOWS_TOOLS / "build"
    shutil.rmtree(build_dir, ignore_errors=True)
    build_dir.mkdir(parents=True)
    source = WINDOWS_TOOLS / "src" / "br" / "lakehouse" / "hadoop" / "BareLocalFileSystem.java"
    base = [javac, "-nowarn", "-encoding", "UTF-8", "-cp", str(hadoop_api), "-d", str(build_dir), str(source)]
    try:
        _run_quiet(base[:1] + ["--release", "8"] + base[1:], "compilar BareLocalFileSystem")
    except RuntimeError:
        _run_quiet(base[:1] + ["-source", "8", "-target", "8"] + base[1:], "compilar BareLocalFileSystem")
    with zipfile.ZipFile(BARE_FS_JAR, "w", zipfile.ZIP_DEFLATED) as zf:
        for cls in build_dir.rglob("*.class"):
            zf.write(cls, cls.relative_to(build_dir).as_posix())
    shutil.rmtree(build_dir, ignore_errors=True)
    log.info("Compilado %s", BARE_FS_JAR)
    return BARE_FS_JAR


def get_spark(settings: Settings, app_name: str = "pipeline-lakehouse"):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    java = ensure_java(settings.spark.get("java_home") or None)
    log.info("Java %s (%s)%s", java.major, java.source, f" em {java.home}" if java.home else "")

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    tmp_dir = settings.lakehouse_dir.parent / "tmp"
    (tmp_dir / "spark").mkdir(parents=True, exist_ok=True)
    log4j = (PROJECT_ROOT / "config" / "log4j2.properties").resolve().as_uri()
    java_opts = f"-Dlog4j.configurationFile={log4j} -Dderby.system.home={(tmp_dir / 'derby').as_posix()}"

    builder = (
        SparkSession.builder.master(settings.spark.get("master", "local[*]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", str(settings.spark.get("driver_memory", "2g")))
        .config("spark.sql.shuffle.partitions", str(settings.spark.get("shuffle_partitions", 4)))
        .config("spark.default.parallelism", str(settings.spark.get("shuffle_partitions", 4)))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", (tmp_dir / "spark").as_posix())
        .config("spark.sql.warehouse.dir", (tmp_dir / "spark-warehouse").as_posix())
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.debug.maxToStringFields", "500")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )

    if IS_WINDOWS and not winutils_available():
        hadoop_home = CACHE_DIR / "hadoop"
        (hadoop_home / "bin").mkdir(parents=True, exist_ok=True)
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        jars = ensure_delta_jars(java) + [ensure_bare_fs_jar(java)]
        log.info("Windows sem winutils.exe: usando modo compatível (BareLocalFileSystem).")
        builder = (
            builder.config("spark.driver.extraClassPath", os.pathsep.join(str(j) for j in jars))
            .config("spark.hadoop.fs.file.impl", BARE_FS_CLASS)
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .config("spark.delta.logStore.file.impl", "io.delta.storage.LocalLogStore")
        )
    else:
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
