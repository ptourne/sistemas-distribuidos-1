import socket
import threading
import multiprocessing
import time
import os
import signal
import sys

HEARTBEAT_INTERVAL = 0.2  # seconds 

def get_udp_addrs(addr_list):
    addrs = []
    for addr in addr_list.split(","):
        addr = addr.strip()
        while True:
            try:
                host, port = addr.split(":")
                addrs.append((host.strip(), int(port.strip())))
                break
            except Exception as e:
                print(f"[heartbeat] Failed to parse UDP address '{addr}': {e}")
    return addrs


def send_heartbeat(name, addr_list, stop_event):

    udp_addrs = get_udp_addrs(addr_list)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while not stop_event.is_set():
        msg = name.encode("utf-8")
        if len(msg) > 255:
            print(f"[heartbeat] ID too long: {len(msg)} bytes")
            sys.exit(1)

        packet = bytes([len(msg)]) + msg

        for addr in udp_addrs:
            try:
                sent = sock.sendto(packet, addr)
                if sent != len(packet):
                    print(f"[heartbeat] Sent {sent}/{len(packet)} bytes to {addr}")
            except Exception as e:
                print(f"[heartbeat] Failed to send to {addr}: {e}")

        time.sleep(HEARTBEAT_INTERVAL)

    sock.close()
    print("[heartbeat] Stopped sending heartbeats")


def start_heartbeat(identifier, addr_list):
    stop_event = multiprocessing.Event()
    process = multiprocessing.Process(
        target=send_heartbeat,
        args=(identifier, addr_list, stop_event),
        daemon=False
    )
    process.start()
    return stop_event, process
