"""Localiza um Java compatível com o Spark 3.5 (8, 11, 17 ou 21, 64 bits)."""

from __future__ import annotations

import glob
import os
import platform
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from lakehouse.utils.logging_utils import get_logger

log = get_logger("java")

SUPPORTED_JAVA = (8, 11, 17, 21)
PREFERENCE = (17, 21, 11, 8)
IS_WINDOWS = platform.system() == "Windows"


@dataclass
class JavaInfo:
    executable: str
    home: str | None
    major: int
    is_64bit: bool
    source: str

    @property
    def supported(self) -> bool:
        return self.major in SUPPORTED_JAVA and self.is_64bit


def _java_exe(home: str | Path) -> Path:
    return Path(home) / "bin" / ("java.exe" if IS_WINDOWS else "java")


def parse_java_version(output: str) -> int | None:
    """'java version "1.8.0_441"' -> 8 ; 'openjdk version "17.0.9"' -> 17."""
    match = re.search(r'version "([^"]+)"', output)
    if not match:
        return None
    version = match.group(1)
    parts = re.split(r"[._\-+]", version)
    try:
        first = int(parts[0])
        return int(parts[1]) if first == 1 and len(parts) > 1 else first
    except ValueError:
        return None


def probe_java(executable: str | Path, home: str | None, source: str) -> JavaInfo | None:
    try:
        proc = subprocess.run(
            [str(executable), "-version"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.SubprocessError):
        return None
    output = (proc.stderr or "") + (proc.stdout or "")
    major = parse_java_version(output)
    if major is None:
        return None
    is_64 = "64-Bit" in output or "64-bit" in output
    return JavaInfo(str(executable), home, major, is_64, source)


def _candidate_homes() -> list[str]:
    patterns: list[str] = []
    home = Path.home()
    if IS_WINDOWS:
        for base in (os.environ.get("ProgramFiles", r"C:\Program Files"), os.environ.get("ProgramW6432", "")):
            if not base:
                continue
            for vendor in ("Java", "Eclipse Adoptium", "Microsoft", "Zulu", "Amazon Corretto", "BellSoft", "Semeru", "OpenJDK"):
                patterns.append(os.path.join(base, vendor, "*"))
        patterns.append(str(home / ".jdks" / "*"))
    elif platform.system() == "Darwin":
        patterns += [
            "/Library/Java/JavaVirtualMachines/*/Contents/Home",
            str(home / "Library/Java/JavaVirtualMachines/*/Contents/Home"),
            "/opt/homebrew/opt/openjdk*/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/opt/openjdk*/libexec/openjdk.jdk/Contents/Home",
        ]
    else:
        patterns += ["/usr/lib/jvm/*", "/usr/java/*", "/opt/java/*", "/opt/jdk*"]
    patterns.append(str(home / ".sdkman" / "candidates" / "java" / "*"))
    homes = []
    for pattern in patterns:
        for candidate in sorted(glob.glob(pattern)):
            if _java_exe(candidate).exists() and candidate not in homes:
                homes.append(candidate)
    return homes


def _activate(info: JavaInfo) -> None:
    if info.home:
        os.environ["JAVA_HOME"] = info.home
        bin_dir = str(Path(info.home) / "bin")
        path = os.environ.get("PATH", "")
        if not path.startswith(bin_dir):
            os.environ["PATH"] = bin_dir + os.pathsep + path


def ensure_java(configured_home: str | None = None) -> JavaInfo:
    """Escolhe o Java e ajusta JAVA_HOME/PATH do processo atual (não altera o sistema)."""
    tried: list[JavaInfo] = []

    if configured_home:
        info = probe_java(_java_exe(configured_home), configured_home, "settings.yaml (spark.java_home)")
        if info is None:
            raise RuntimeError(f"spark.java_home aponta para um Java inválido: {configured_home}")
        if not info.supported:
            log.warning("Java %s configurado em spark.java_home não é suportado pelo Spark 3.5.", info.major)
        _activate(info)
        return info

    env_home = os.environ.get("JAVA_HOME")
    if env_home and _java_exe(env_home).exists():
        info = probe_java(_java_exe(env_home), env_home, "JAVA_HOME")
        if info and info.supported:
            _activate(info)
            return info
        if info:
            tried.append(info)

    on_path = shutil.which("java")
    if on_path:
        info = probe_java(on_path, None, "PATH")
        if info and info.supported:
            return info
        if info:
            tried.append(info)

    found = [i for i in (probe_java(_java_exe(h), h, "busca automática") for h in _candidate_homes()) if i]
    supported = [i for i in found if i.supported]
    if supported:
        supported.sort(key=lambda i: PREFERENCE.index(i.major))
        chosen = supported[0]
        if tried:
            log.info(
                "Java padrão (versão %s) não é compatível com o Spark 3.5; usando JDK %s em %s",
                tried[0].major, chosen.major, chosen.home,
            )
        _activate(chosen)
        return chosen

    detail = ", ".join(f"Java {i.major}{'' if i.is_64bit else ' (32 bits)'} via {i.source}" for i in tried + found)
    raise RuntimeError(
        "Nenhum Java compatível encontrado. O Spark 3.5 precisa de Java 8, 11, 17 ou 21 (64 bits). "
        f"Encontrados: {detail or 'nenhum'}. Instale um JDK 17 (ex.: Eclipse Temurin) e/ou "
        "configure spark.java_home em config/settings.yaml."
    )
