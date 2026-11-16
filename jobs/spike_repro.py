"""Reproduce the reference spike's experiments A, B and C on the pinned Restate server (spec 010,
AC-010-22 … -25).

`make spike` runs it. The spike's files stay unedited under reference/restate-ems-spike/. Only two
things come from outside: a throwaway virtualenv with the packages of the spike's own
requirements.txt (restate-sdk 1.0.5), and the ctl.sh verbs of infra/spike/ctl.sh, copied into the
run directory, where the spike calls ./ctl.sh. They run the server image pinned in
infra/docker-compose.yml as a container of its own, separate from the stack's.

The run directory experiments/results/spike-<UTC>/ gets the spike's results.json, ctl.log (one line
per verb), the experiment output and manifest.json (hardware, image, frozen packages, git commit,
verdict). The exit code is 0 only when A and C show no mismatch and B shows none after both kills,
which ctl.log confirms. Nothing is written into reference/ (PYTHONDONTWRITEBYTECODE, and the spike
runs from the run directory).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from ems.experiments.stack import compose_images, docker_info, hardware, port_in_use

REPO = Path(__file__).resolve().parents[1]
SPIKE = REPO / "reference" / "restate-ems-spike"
CTL = REPO / "infra" / "spike" / "ctl.sh"
REQUIRED_PORTS = (8080, 9070, 9080, 9081)
EXPERIMENTS = {"A": "A_baseline", "B": "B_crash", "C": "C_dynamic_routing"}
STEPS = 96  # the spike's quarter-hours per day

app = typer.Typer(add_completion=False)


def ports_in_use(ports: Iterable[int], host: str = "127.0.0.1") -> list[int]:
    """The ports something listens on (decided by binding, like the harness)."""
    return [port for port in ports if port_in_use(port, host)]


def _fields(line: str) -> tuple[str, dict[str, str]]:
    parts = line.split()
    verb = parts[1] if len(parts) > 1 else ""
    return verb, dict(p.split("=", 1) for p in parts[2:] if "=" in p)


def verdict(results: Mapping[str, Any], ctl_log: str) -> list[str]:
    """Why the reproduction fails; empty when it passes (AC-010-24)."""
    reasons = [f"missing {name}" for name in EXPERIMENTS.values() if name not in results]
    for name in ("A_baseline", "C_dynamic_routing"):
        if name in results:
            check = results[name].get("verification", {})
            for key in ("mismatches", "meter_vs_selfconsumption_mismatch", "out_of_order"):
                if check.get(key) != 0:
                    reasons.append(f"{name}: {key} = {check.get(key)}")
    if "B_crash" in results:
        crash = results["B_crash"]
        check = crash.get("verification", {})
        for key in ("mismatches", "out_of_order"):
            if check.get(key) != 0:
                reasons.append(f"B_crash: {key} = {check.get(key)}")
        expected = int(crash.get("households", 0)) * len(crash.get("strategies", [])) * STEPS
        if check.get("events_total") != expected:
            reasons.append(
                f"B_crash: events_total = {check.get('events_total')}, expected {expected}"
            )
    entries = [_fields(line) for line in ctl_log.splitlines() if line.strip()]
    if not any(verb == "kill-svc1" and fields.get("pids") for verb, fields in entries):
        reasons.append("ctl.log shows no kill of the SDK service (kill-svc1 with pids)")
    if not any(verb == "kill-server" and fields.get("exit") == "137" for verb, fields in entries):
        reasons.append("ctl.log shows no SIGKILL of the server (kill-server exit=137)")
    return reasons


def _wait(predicate: Any, timeout: float, what: str) -> None:
    start = time.monotonic()
    while not predicate():
        if time.monotonic() - start > timeout:
            raise TimeoutError(f"{what} after {timeout:.0f} s")
        time.sleep(0.5)


def _healthy() -> bool:
    try:
        admin = httpx.get("http://127.0.0.1:9070/health", timeout=2.0)
        ingress = httpx.get("http://127.0.0.1:8080/restate/health", timeout=2.0)
    except httpx.HTTPError:
        return False
    return admin.status_code == 200 and ingress.status_code == 200


@app.command()
def main(
    experiments: Annotated[
        list[str] | None, typer.Argument(help="Spike experiments to run")
    ] = None,
    out: Annotated[Path, typer.Option(help="Results root")] = Path("experiments/results"),
    timeout_min: Annotated[int, typer.Option(help="Limit for the spike's own run")] = 60,
) -> None:
    chosen = experiments or list(EXPERIMENTS)
    busy = ports_in_use(REQUIRED_PORTS)
    if busy:
        typer.echo(
            f"ports in use: {busy}. Stop the stack (`make down`) and any `make dev` first.",
            err=True,
        )
        raise typer.Exit(2)
    uv = shutil.which("uv")
    if uv is None:
        typer.echo("uv is not on PATH", err=True)
        raise typer.Exit(2)
    run_dir = (out / f"spike-{datetime.now(UTC):%Y%m%dT%H%M%SZ}").resolve()
    run_dir.mkdir(parents=True)
    shutil.copy2(CTL, run_dir / "ctl.sh")
    venv = run_dir / "venv"
    python = venv / "bin" / "python"
    subprocess.run([uv, "venv", "--quiet", "--python", "3.13", str(venv)], check=True)
    subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--quiet",
            "--python",
            str(python),
            "-r",
            str(SPIKE / "requirements.txt"),
        ],
        check=True,
    )
    packages = subprocess.run(
        [uv, "pip", "freeze", "--python", str(python)], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    image = compose_images()["restate"]
    env = {
        **os.environ,
        "PATH": f"{venv / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}",
        "VIRTUAL_ENV": str(venv),
        "PYTHONDONTWRITEBYTECODE": "1",
        "SPIKE_DIR": str(SPIKE),
        "SPIKE_RESTATE_IMAGE": image,
    }

    def ctl(*args: str) -> None:
        subprocess.run(["./ctl.sh", *args], cwd=run_dir, env=env, check=False, timeout=120)

    started = datetime.now(UTC)
    returncode: int | None = None
    try:
        ctl("wipe")
        ctl("server")
        _wait(_healthy, 180, "the spike's Restate server is not healthy")
        ctl("svc1")
        _wait(lambda: port_in_use(9080), 60, "the spike's service does not listen on 9080")
        ctl("register", "http://127.0.0.1:9080")
        with (run_dir / "experiments.log").open("w") as log:
            proc = subprocess.run(
                [str(python), str(SPIKE / "experiments.py"), *chosen],
                cwd=run_dir,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=timeout_min * 60,
                check=False,
            )
        returncode = proc.returncode
    finally:
        ctl("wipe")
        shutil.rmtree(venv, ignore_errors=True)
    finished = datetime.now(UTC)
    results_path = run_dir / "results.json"
    results = json.loads(results_path.read_text()) if results_path.exists() else {}
    ctl_log = (run_dir / "ctl.log").read_text() if (run_dir / "ctl.log").exists() else ""
    reasons = verdict(results, ctl_log) if set(chosen) == set(EXPERIMENTS) else []
    if returncode != 0:
        reasons.append(f"experiments.py exited with {returncode}")
    from ems.ingest.runner import _git_commit

    manifest = {
        "kind": "spike-reproduction",
        "experiments": chosen,
        "spike_dir": str(SPIKE.relative_to(REPO)),
        "server_image": image,
        "packages": packages,
        "started_utc": started.isoformat(),
        "finished_utc": finished.isoformat(),
        "git_commit": _git_commit(REPO),
        "hardware": hardware(),
        "docker": docker_info(),
        "verdict": reasons or "pass",
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    typer.echo(f"wrote {run_dir}")
    if reasons:
        typer.echo("FAIL:\n  " + "\n  ".join(reasons), err=True)
        raise typer.Exit(1)
    typer.echo("PASS: A and C without mismatches, B without mismatches after both kills")


if __name__ == "__main__":
    app()
