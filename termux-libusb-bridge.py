#!/usr/bin/python3

import argparse
import array
import dataclasses
import json
import os
import shlex
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
import traceback
import logging
import signal

# Set up logging
logging.basicConfig(
    filename='/var/log/libusb_bridge.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
)

logging.info('Starting libusb_bridge daemon...')

def send_fds(sock, msg, fds):
    logging.debug(f'Sending fds: {fds}')
    return sock.sendmsg(
        [msg], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))])

def recv_fds(sock, msglen, maxfds):
    fds = array.array('i')  # Array of ints
    msg, ancdata, flags, addr = sock.recvmsg(
        msglen, socket.CMSG_LEN(maxfds * fds.itemsize))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds.frombytes(cmsg_data[:len(cmsg_data) -
                                    (len(cmsg_data) % fds.itemsize)])
    logging.debug(f'Received fds: {list(fds)}')
    return msg, list(fds)

def list_devices():
    args = ['termux-usb', '-l']
    p = subprocess.run(args, capture_output=True)
    devices = json.loads(p.stdout)
    logging.debug(f'Devices: {devices}')
    return devices

def open_device(path: os.PathLike):
    s1, s2 = socket.socketpair(socket.AF_UNIX)
    pair_fd = s1.fileno()
    callback = [sys.executable, __file__, '--fds', str(pair_fd)]
    args = ['termux-usb', '-r', '-e', shlex.join(callback), os.fspath(path)]
    p = subprocess.run(args, pass_fds=[pair_fd])
    if p.returncode != 0:
        logging.error(f'Failed to run termux-usb: {p.stderr}')
        return None

    s2.settimeout(0.2)
    try:
        msg, [dev_fd] = recv_fds(s2, 1, 1)
        logging.debug(f'Opened device {path}: fd {dev_fd}')
    except (ValueError, socket.timeout) as e:
        logging.error(f'Error receiving fd: {e}')
        return None

    return dev_fd

class RequestHandler(socketserver.StreamRequestHandler):

    def read_str(self):
        data = self.rfile.read(2)
        if not data:
            return None
        if len(data) != 2:
            raise OSError('bad read')
        l, = struct.unpack('!H', data)
        if not l:
            return ''
        data = self.rfile.read(l)
        return data.decode('utf8')

    def write_str(self, s):
        data = s.encode('utf8')
        self.wfile.write(struct.pack('!H', len(data)) + data)

    def handle(self):
        s = self.read_str()
        logging.debug(f'Received string: {s}')

        if s is None:
            logging.debug('No data received')
            return

        if not s:
            logging.debug('Empty string received, listing devices')
            for d in self.server.get_devices():
                self.write_str(d)
            self.write_str('')
            return

        fd = self.server.get_device(s)
        logging.debug(f'Got device fd: {fd} for path: {s}')
        if fd is None:
            logging.error(f'Failed to get device for {s}')
            return

        r = send_fds(self.request, b'\x00', [fd])
        logging.debug(f'send_fds returned: {r}')

        try:
            self.read_str()  # Wait for closing by the client...
            logging.debug('Client closed connection')
        except Exception as e:
            logging.debug(f'Exception while waiting for client to close: {e}')

class ThreadingUnixStreamServer(socketserver.ThreadingMixIn,
                                socketserver.UnixStreamServer):
    pass

@dataclasses.dataclass
class FlushingCacheEntry:
    value: ...
    atime: float

class FlushingCache:

    def __init__(self, max_age, expire_callback):
        self.max_age = max_age
        self.expire_callback = expire_callback
        self.lock = threading.Lock()
        self.items = dict()
        self.flush_thread = threading.Thread(target=self.flusher)
        self.flush_thread.start()

    def get(self, key, default):
        with self.lock:
            if key not in self.items:
                return default
            entry = self.items[key]
            entry.atime = time.time()
            return entry.value

    def __setitem__(self, key, value):
        with self.lock:
            self.items[key] = FlushingCacheEntry(value, time.time())

    def flusher(self):
        while True:
            expired = []
            deadline = time.time() - self.max_age
            with self.lock:
                for key, entry in list(self.items.items()):
                    if entry.atime < deadline:
                        del self.items[key]
                        expired.append((key, entry.value))
            for key, value in expired:
                try:
                    self.expire_callback(key, value)
                except Exception as e:
                    logging.error(f'Error in expire callback: {e}')
                    traceback.print_exc()
            time.sleep(self.max_age / 5)

class Server(ThreadingUnixStreamServer):

    def __init__(self, server_address):
        super().__init__(server_address, RequestHandler)
        self.device_cache = FlushingCache(.3, self.entry_expired)

    def get_devices(self):
        devices = self.device_cache.get(None, None)
        if devices is None:
            devices = list_devices()
            self.device_cache[None] = devices
        return devices

    def get_device(self, path: os.PathLike):
        fspath = os.fspath(path)
        fd = self.device_cache.get(fspath, None)
        if fd is None:
            fd = open_device(path)
            if fd:
                self.device_cache[fspath] = fd
        return fd

    def entry_expired(self, key, value):
        if key:
            os.close(value)

def handle_signal(signal_number, frame):
    logging.info(f'Received signal {signal_number}, shutting down.')
    sys.exit(0)

def daemonize():
    # Perform the first fork
    try:
        pid = os.fork()
        if pid > 0:
            # Exit the first parent
            sys.exit(0)
    except OSError as e:
        logging.error(f'Fork #1 failed: {e}')
        sys.exit(1)

    # Decouple from parent environment
    os.chdir('/')
    os.setsid()
    os.umask(0)

    # Perform the second fork
    try:
        pid = os.fork()
        if pid > 0:
            # Exit the second parent
            sys.exit(0)
    except OSError as e:
        logging.error(f'Fork #2 failed: {e}')
        sys.exit(1)

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as read_null, open('/dev/null', 'a+') as write_null:
        os.dup2(read_null.fileno(), sys.stdin.fileno())
        os.dup2(write_null.fileno(), sys.stdout.fileno())
        os.dup2(write_null.fileno(), sys.stderr.fileno())

def main(args):
    if args.fds:
        s = socket.fromfd(args.fds[0], socket.AF_UNIX, socket.SOCK_STREAM)
        r = send_fds(s, b'@', [args.fds[1]])
        if r != 1:
            logging.error('Failed to send fds')
            return 1
        return 0

    logging.info('Starting daemon')
    srv = Server('\x00' + args.socket_name)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    srv.serve_forever()

parser = argparse.ArgumentParser()
parser.add_argument(
    '-s',
    '--socket_name',
    default='com.termux.libusb',
    help=('abstract unix socket name of the service to bridge to termux '
          '(default: com.termux.libusb)'))
parser.add_argument('--fds', nargs=2, type=int, help=argparse.SUPPRESS)
parser.add_argument('--daemon', action='store_true', help='Run as a daemon')

if __name__ == '__main__':
    args = parser.parse_args()
    if args.daemon:
        logging.info('Daemonizing the process')
        daemonize()
        # Redirect logs to file after daemonizing
        logging.getLogger().handlers = []
        logging.basicConfig(
            filename='/var/log/libusb_bridge.log',
            level=logging.DEBUG,
            format='%(asctime)s %(levelname)s %(message)s',
        )
        logging.info('Daemon started')
        try:
            main(args)
        except Exception as e:
            logging.exception("Exception in daemon:")
            sys.exit(1)
    else:
        try:
            main(args)
        except Exception as e:
            logging.exception("Exception in daemon:")
            sys.exit(1)
