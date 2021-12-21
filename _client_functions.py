import hashlib
import logging
import math
import os
import random
import select
import socket
import struct
from _shared_functions import *
from collections import OrderedDict
from collections import namedtuple
from threading import Thread


# ---------- TEST/DEBUG ----------
SIMULATE_PACKET_LOSS = False
SIMULATE_PACKET_LOSS_PCT = 0.05
# --------------------------------


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s CLIENT [%(levelname)s] %(message)s',)
log = logging.getLogger()


class SocketException(Exception): pass
class FileException(Exception): pass
class WindowSizeException(Exception): pass


class Client(object):
    def __init__(self,
                 client_ip="127.0.0.1",
                 client_port=800,
                 server_ip="localhost",
                 server_port=531,
                 seq_bits=16,
                 window_size=1,
                 client_data_folder=os.path.join(os.getcwd(), "client")):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_port = server_port
        self.server_ip = server_ip
        self.client_ip = client_ip
        self.client_port = client_port
        self.seq_num_bits = seq_bits
        self.window_size = window_size
        self.client_data_folder = client_data_folder

    def open(self):
        print("Opening UDP Socket on %s:%d", self.client_ip, self.client_port, file=sys.stderr)

    def receive(self,
                file_name,
                server_ip="localhost",
                server_port=531, client_ip="127.0.0.1",
                client_port=800,
                timeout=10):

        self.file_handle = None

        if SIMULATE_PACKET_LOSS:
            print_star_line()
            print(f"{color.OKBLUE}{color.BOLD}{color.UNDERLINE}SIMULATION SETTINGS{color.END}", file=sys.stderr)
            print(f"{color.BLACK}{color.BOLD}PACKET LOSS: {color.END}"
                  f"{color.BLACK}({SIMULATE_PACKET_LOSS_PCT * 100}%){color.END}"
                  , file=sys.stderr)
            print_star_line()
        print(f"{color.BLACK}Opening UDP Socket on {self.client_ip}:{self.client_port}{color.END}", file=sys.stderr)
        self.client_socket.bind((self.client_ip, self.client_port))
        log.info("Success")

        print(f"{color.BLACK}Receiving Data from Server ({self.server_ip}){color.END}", file=sys.stderr)
        file_path = os.path.join(self.client_data_folder, file_name)

        self.client_socket.sendto(file_name.encode('utf-8'), (self.server_ip, self.server_port))


        try:
            print(f"Writing Payload to File ({file_path})", file=sys.stderr)
            print_star_line()
            self.file_handle = open(file_path, "wb")
        except IOError as e:
            log.debug(e)
            raise FileException(f"Unable to Create File Handle ({file_path})")

        window = Window(self.seq_num_bits, self.window_size)

        packet_handler = PacketHandler(self.file_handle, self.client_socket, server_ip,
                                       server_port, self.client_ip, self.client_port, window, timeout)
        packet_handler.start()
        packet_handler.join()

    def close(self):
        try:
            if self.file_handle:
                self.file_handle.close()
        except IOError as e:
            log.debug(e)
            raise FileException("Unable to Close File Handle")

        try:
            if self.client_socket:
                self.client_socket.close()
        except Exception as e:
            raise SocketException(f"Unable to Close UDP Socket (%s:%d)"
                                  % (self.client_ip, self.client_port))


class Window(object):
    def __init__(self, seq_num_bits, window_size=None):
        self.expected_packet = 0
        self.max_seq = int(math.pow(2, seq_num_bits))
        if window_size is None:
            self.max_window_size = int(math.pow(2, seq_num_bits - 1))
        else:
            if window_size > int(math.pow(2, seq_num_bits - 1)):
                raise WindowSizeException("Invalid Window Size")
            else:
                self.max_window_size = window_size
        self.lastPkt = self.max_window_size - 1
        self.receipt_window = OrderedDict()
        self.is_receipt = False

    def expected_packet(self):
        return self.expected_packet

    def lastPacket(self):
        return self.lastPkt

    def ooo(self, key):
        if self.expected_packet > self.lastPacket():
            if self.expected_packet > key > self.lastPacket():
                return True
        else:
            if key < self.expected_packet or key > self.lastPacket():
                return True
        return False

    def exist(self, key):
        if key in self.receipt_window and self.receipt_window[key] is not None:
            return True
        return False

    def store(self, receivedPacket):
        if not self.expected(receivedPacket.Seq):
            seq = self.expected_packet

            while seq != receivedPacket.Seq:
                if seq not in self.receipt_window:
                    self.receipt_window[seq] = None

                seq += 1
                if seq >= self.max_seq:
                    seq %= self.max_seq

        self.receipt_window[receivedPacket.Seq] = receivedPacket

    def expected(self, seq):
        if seq == self.expected_packet:
            return True
        return False

    def __next__(self):
        packet = None

        if len(self.receipt_window) > 0:
            next_packet = list(self.receipt_window.items())[0]

            if next_packet[1] is not None:
                packet = next_packet[1]

                del self.receipt_window[next_packet[0]]

                self.expected_packet = next_packet[0] + 1
                if self.expected_packet >= self.max_seq:
                    self.expected_packet %= self.max_seq

                self.lastPkt = self.expected_packet + self.max_window_size - 1
                if self.lastPkt >= self.max_seq:
                    self.lastPkt %= self.max_seq

        return packet

    def receipt(self):
        return self.is_receipt

    def start_receipt(self):
        self.is_receipt = True


def get_hashcode(data):
    hashcode = hashlib.md5()
    hashcode.update(repr(data).encode())

    return hashcode.digest()


def create_packet(ack):
    ack_num = struct.pack('=I', ack.ack_num)
    checksum = struct.pack('=16s', ack.Checksum)
    rawAck = ack_num + checksum
    return rawAck


def parse(received_packet):
    header = received_packet[0:6]
    data = received_packet[6:]

    seq = struct.unpack('=I', header[0:4])[0]
    checksum = struct.unpack('=H', header[4:])[0]

    packet = PacketHandler.PACKET(Seq=seq, Checksum=checksum, Data=data)

    return packet


def checksum(data):
    data = data.decode('utf-8')
    if (len(data) % 2) != 0:
        data += "0"

    s = 0

    for i in range(0, len(data), 2):
        data_segment = ord(data[i]) + (ord(data[i + 1]) << 8)
        s += data_segment
        s = (s & 0xffff) + (s >> 16)

    return ~s & 0xffff


def is_corrupt(receivedPacket):
    if checksum(receivedPacket.Data) != receivedPacket.Checksum:
        return True
    else:
        return False


class PacketHandler(Thread):
    PACKET = namedtuple("Packet", ["Seq", "Checksum", "Data"])
    ACK = namedtuple("ACK", ["ack_num", "Checksum"])

    def __init__(self,
                 file_handle,
                 client_socket,
                 server_ip,
                 server_port,
                 client_ip,
                 client_port,
                 window,
                 timeout=10,
                 buffer_size=32736):
        Thread.__init__(self)
        self.file_handle = file_handle
        self.client_socket = client_socket
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.window = window
        self.timeout = timeout
        self.buffer_size = buffer_size

    def run(self):
        chance = 0
        while True:
            ready = select.select([self.client_socket], [], [], self.timeout)

            if not ready[0]:
                if not self.window.receipt():
                    continue
                else:
                    if chance == 1:
                        log.info("File Transfer Complete")
                        break
                    else:
                        chance += 1
                        continue
            else:
                chance = 0
                if not self.window.receipt():
                    self.window.start_receipt()

            try:
                received_packet, _ = self.client_socket.recvfrom(self.buffer_size)
            except Exception as e:
                log.exception("Unable to Receive Packet")
                log.debug(e)
                raise SocketException("Unable to Receive Packet")

            received_packet = parse(received_packet)

            if is_corrupt(received_packet):
                log.warning(f"{color.HEADER}{color.BOLD}[SEQ=%d] "
                            f"{color.FAIL}Packet Checksum Invalid{color.END}"
                            , received_packet.Seq)
                continue

            if self.window.ooo(received_packet.Seq):
                log.warning("[SEQ=%d] Unexpected Packet Sequence Number",
                            received_packet.Seq)

                log.info("[ACK=%d] ACK Sent", received_packet.Seq)
                self.rdt_send(received_packet.Seq)
                continue

            if self.simulate_packet_loss():
                log.debug("[SIMULATING PACKET LOSS]")
                log.error("[SEQ=%d] Packet Lost", received_packet.Seq)
                continue

            if self.window.exist(received_packet.Seq):
                log.warning("[SEQ=%d] Duplicate Packet Discarded", received_packet.Seq)
                continue
            else:
                print(f"{color.OKGREEN}{color.BOLD}[SEQ={received_packet.Seq}] Packet Received {color.END}"
                      f"{color.OKBLUE}({self.server_port}) ➞ ({self.client_port}) "
                      f"Len={len(received_packet.Data)}{color.END}", file=sys.stderr)

                self.window.store(received_packet)

                log.info(f"[SEQ={received_packet.Seq}] ACK Sent")
                self.rdt_send(received_packet.Seq)

            if self.window.expected(received_packet.Seq):
                self.write_received_data_to_file()

    def rdt_send(self, ack_num):
        ack = PacketHandler.ACK(ack_num=ack_num, Checksum=get_hashcode(ack_num))
        ack = create_packet(ack)
        self.udt_send(ack)

    def udt_send(self, ack):
        try:
            self.client_socket.sendto(ack, (self.server_ip, self.server_port))
        except Exception as e:
            log.error("Packet Transmission Failed")
            log.debug(e)
            raise SocketException("Packet Transmission Failed (%s:%d)" % (self.server_ip, self.server_port))

    def simulate_packet_loss(self):
        if SIMULATE_PACKET_LOSS:
            r = random.randint(1, 100)

            if r <= SIMULATE_PACKET_LOSS_PCT * 100:
                return True
            else:
                return False
        return False

    def write_received_data_to_file(self):
        while True:
            packet = next(self.window)

            if packet:
                try:
                    self.file_handle.write(packet.Data)
                except IOError as e:
                    log.error("[I/O] Unable to Write to File")
                    log.debug(e)
                    raise FileException("[I/O] Unable to Write to File")
            else:
                break
