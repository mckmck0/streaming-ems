"""Clean re-run of B, D, D2 after fixing ctl.sh (hypercorn workers are now really killed)."""
import os, subprocess, time
import experiments as e

def restart(capped: bool):
    e.ctl("kill-svc1"); e.ctl("kill-svc2"); time.sleep(1)
    os.environ.pop("EMS_RETRY_MAX_INTERVAL_S", None)
    if capped:
        os.environ["EMS_RETRY_MAX_INTERVAL_S"] = "1"
    e.ctl("svc1"); e.ctl("svc2"); time.sleep(4)
    e.ctl("register-force", "http://127.0.0.1:9080"); e.ctl("register-force", "http://127.0.0.1:9081")

restart(capped=False)
e.exp_crash(prefix="f", name="B_crash")
e.exp_timers(key="d0010", name="D_durable_timers", zone="Z10")
restart(capped=True)
e.exp_timers(key="d0011", name="D2_durable_timers_capped_retry", zone="Z11")
print("RERUN DONE")
