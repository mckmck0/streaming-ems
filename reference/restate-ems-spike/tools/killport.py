"""Kill every process holding a listening TCP socket on the given port (Linux /proc).
Needed because hypercorn serves from a multiprocessing child that `pkill -f` does not match."""
import glob, os, signal, sys
port = "%04X" % int(sys.argv[1])
inodes = set()
for f in ("/proc/net/tcp", "/proc/net/tcp6"):
    if os.path.exists(f):
        for line in open(f).readlines()[1:]:
            p = line.split()
            if p[1].endswith(":" + port) and p[3] == "0A":
                inodes.add(p[9])
pids = set()
for fd in glob.glob("/proc/[0-9]*/fd/*"):
    try:
        if os.readlink(fd)[8:-1] in inodes:
            pids.add(int(fd.split("/")[2]))
    except OSError:
        pass
for pid in pids:
    os.kill(pid, signal.SIGKILL)
print("killed", sorted(pids))
