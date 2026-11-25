import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PORT = 9092
RESULTS = {}
ERRORS = []
BARRIER = threading.Barrier(4)


def client(number):
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=4) as conn:
            conn.settimeout(4)
            BARRIER.wait(timeout=4)
            time.sleep(0.2)
            message = f"Client {number}: hello!".encode("utf-8")
            conn.sendall(message)
            response = b""
            while len(response) < len(message):
                part = conn.recv(len(message) - len(response))
                if not part:
                    raise RuntimeError("Connection closed early")
                response += part
            if response != message:
                raise AssertionError(f"Client {number} received wrong data")
            RESULTS[number] = response.decode("utf-8")
            time.sleep(0.15)
    except Exception as exc:
        ERRORS.append(f"Client {number}: {exc}")


def main():
    process = subprocess.Popen(
        [str(ROOT / "server")], cwd=ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    try:
        greeting = process.stdout.readline().strip()
        if "Server listening" not in greeting:
            raise RuntimeError(f"Server failed to start: {greeting}")
        threads = [threading.Thread(target=client, args=(i,)) for i in range(1, 4)]
        for thread in threads:
            thread.start()
        BARRIER.wait(timeout=4)
        for thread in threads:
            thread.join(timeout=6)
        if ERRORS or any(thread.is_alive() for thread in threads):
            raise RuntimeError("; ".join(ERRORS) or "Clients did not finish")
        if len(RESULTS) != 3:
            raise AssertionError("Not all clients received replies")
    finally:
        process.send_signal(signal.SIGINT)
        output, _ = process.communicate(timeout=5)

    if process.returncode != 0:
        raise RuntimeError(f"Server returned {process.returncode}: {output}")
    if "Shutdown: active=0, total=3, finished=3, peak=3" not in output:
        raise AssertionError(f"Incorrect server statistics: {output}")

    for number, message in sorted(RESULTS.items()):
        print(f"Client {number}: sent and received '{message}'")
    print("Three concurrent clients: PASS")
    print("Server shutdown and descriptors: PASS")
    print("Server output:")
    print(greeting)
    print(output.strip())


if __name__ == "__main__":
    main()
