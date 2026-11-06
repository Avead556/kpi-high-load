import argparse
import collections
import errno
import socket
import subprocess


def main():
    parser = argparse.ArgumentParser(description="open many TCP connections at once and count the errors")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8018)
    parser.add_argument("--count", type=int, default=17000)
    args = parser.parse_args()
    print(subprocess.run(["sysctl", "net.inet.ip.portrange.first", "net.inet.ip.portrange.last"],
                         capture_output=True, text=True).stdout.strip())
    sockets, errors = [], collections.Counter()
    for _ in range(args.count):
        try:
            sockets.append(socket.create_connection((args.host, args.port), timeout=3))
        except OSError as exc:
            errors[f"{type(exc).__name__} errno {exc.errno} {errno.errorcode.get(exc.errno or 0, '')}: {exc.strerror}"] += 1
    print(f"requested {args.count}, opened {len(sockets)}")
    for message, n in errors.most_common():
        print(f"{n} x {message}")
    for s in sockets:
        s.close()


if __name__ == "__main__":
    main()
