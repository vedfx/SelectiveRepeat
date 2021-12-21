import hashlib
import logging
import math
import os
import random
import select
import socket
import struct
import time
from collections import OrderedDict
from collections import namedtuple
from threading import Lock
from threading import Thread

from _shared_functions import *

# ---------- TEST/DEBUG ----------
SIMULATE_ACK_LOSS = False
SIMULATE_ACK_LOSS_PCT = 0.05

SIMULATE_FILE_CORRUPTION = False
SIMULATE_FILE_CORRUPTION_PCT = 0.06
# --------------------------------


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s SERVER [%(levelname)s] %(message)s', )
log = logging.getLogger()

LOCK = Lock()


class SocketException(Exception): pass
class FileException(Exception): pass
class WindowSizeException(Exception): pass


class Server(object):
    def __init__(self,
                 server_ip="localhost",
                 server_port=531,
                 seq_num_bits=16,
                 window_size=None,
                 max_segment_size=32736,
                 server_data_folder=os.path.join(os.getcwd(), "server")):
        self.client_ip = server_ip
        self.client_port = server_port
        self.seq_num_bits = seq_num_bits
        self.window_size = window_size
        self.max_segment_size = max_segment_size
        self.server_data_folder = server_data_folder

    def open(self):
        print_star_line()

        if SIMULATE_ACK_LOSS or SIMULATE_FILE_CORRUPTION:
            print(f"{color.OKBLUE}{color.BOLD}{color.UNDERLINE}SIMULATION SETTINGS{color.END}", file=sys.stderr)

        if SIMULATE_ACK_LOSS:
            print(f"{color.BLACK}{color.BOLD}ACK LOSS: {color.END}"
                  f"{color.BLACK}({SIMULATE_ACK_LOSS_PCT * 100}%){color.END}"
                  , file=sys.stderr)
            if not SIMULATE_FILE_CORRUPTION:
                print_star_line()

        if SIMULATE_FILE_CORRUPTION:
            print(f"{color.BLACK}{color.BOLD}FILE CORRUPTION: {color.END}"
                  f"{color.BLACK}({SIMULATE_FILE_CORRUPTION_PCT * 100}%){color.END}"
                  , file=sys.stderr)
            print_star_line()

        print(f"{color.BLACK}Opening UDP Socket on "
              f"{self.client_ip}:{self.client_port}{color.END}", file=sys.stderr)

        while 1:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.server_socket.bind((self.client_ip, self.client_port))
            log.info("Success")
            print(f"{color.BLACK}Awaiting Incoming Connections...{color.END}", file=sys.stderr)
            fileName, address = self.server_socket.recvfrom(4096)
            if fileName:
                client_ip = '127.0.0.1'
                client_port = 800
                total_packets = "ALL"
                timeout = 10
                self.send(fileName,
                          client_ip,
                          client_port,
                          total_packets,
                          timeout)

    def send(self,
             file_name,
             client_ip='127.0.0.1',
             client_port=800,
             total_packets='ALL',
             timeout=10):

        file_name = file_name.decode('utf-8')
        log.info("Transmitting File (%s) to Client", file_name)
        file_name = os.path.join(self.server_data_folder, file_name)

        if not os.path.exists(file_name):
            raise FileException("File Not Found (%s)" % file_name)

        window = Window(self.seq_num_bits,
                        self.window_size)

        packet_handler = PacketHandler(file_name,
                                       self.server_socket,
                                       self.client_ip,
                                       self.client_port,
                                       client_ip,
                                       client_port,
                                       window,
                                       self.max_segment_size,
                                       total_packets,
                                       timeout)

        ack_handler = ACKHandler(self.server_socket,
                                 self.client_ip,
                                 self.client_port,
                                 client_ip,
                                 client_port,
                                 window)

        packet_handler.start()
        ack_handler.start()

        packet_handler.join()
        ack_handler.join()
        log.info("File Transfer Complete")
        self.close()
        self.open()

    def close(self):
        try:
            if self.server_socket:
                self.server_socket.close()
                log.info("Closing Connection")
        except Exception as e:
            log.error("Unable to Close UDP Socket")
            log.debug(e)
            raise SocketException("Unable to Close UDP Socket (%s:%d)" % (self.client_ip, self.client_port))


class Window(object):
    def __init__(self, seq_num_bits, window_size=None):
        self.expected_ack = 0
        self.next_seq = 0
        self.next_packet = 0
        self.max_seq = int(math.pow(2, seq_num_bits))
        if window_size is None:
            self.max_window_size = int(math.pow(2, seq_num_bits - 1))
        else:
            if window_size > int(math.pow(2, seq_num_bits - 1)):
                raise WindowSizeException("Invalid Window Size")
            else:
                self.max_window_size = window_size
        self.transmission_window = OrderedDict()
        self.is_packet_transmission = True

    def expectedACK(self):
        return self.expected_ack

    def maxSequenceNumber(self):
        return self.max_seq

    def empty(self):
        if len(self.transmission_window) == 0:
            return True
        return False

    def full(self):
        if len(self.transmission_window) >= self.max_window_size:
            return True
        return False

    def exist(self, key):
        if key in self.transmission_window:
            return True
        return False

    def __next__(self):
        return self.next_packet

    def consume(self, key):
        with LOCK:
            self.transmission_window[key] = [None, False]

            self.next_seq += 1
            if self.next_seq >= self.max_seq:
                self.next_seq %= self.max_seq

            self.next_packet += 1

    def start(self, key):
        with LOCK:
            self.transmission_window[key][0] = time.time()

    def restart(self, key):
        with LOCK:
            self.transmission_window[key][0] = time.time()

    def stop(self, key):
        if self.exist(key):
            self.transmission_window[key][0] = None

        if key == self.expected_ack:
            for k, v in list(self.transmission_window.items()):
                if v[0] == None and v[1] == True:
                    del self.transmission_window[k]
                else:
                    break

            if len(self.transmission_window) == 0:
                self.expected_ack = self.next_seq
            else:
                self.expected_ack = list(self.transmission_window.items())[0][0]

    def start_time(self, key):
        return self.transmission_window[key][0]

    def unacked(self, key):
        if self.exist(key) and self.transmission_window[key][1] == False:
            return True
        return False

    def make_acked(self, key):
        with LOCK:
            self.transmission_window[key][1] = True

    def stop_transmission(self):
        self.is_packet_transmission = False

    def transmit(self):
        return self.is_packet_transmission


class PacketHandler(Thread):
    HEADER_LENGTH = 8
    PACKET = namedtuple("Packet", ["SequenceNumber", "Checksum", "Data"])

    def __init__(self,
                 file_name,
                 server_socket,
                 server_ip,
                 server_port,
                 client_ip,
                 client_port,
                 window,
                 max_segment_size=32736,
                 totalPackets="ALL",
                 timeout=10,
                 thread_name="PacketHandler",
                 buffer_size=2048):
        Thread.__init__(self)
        self.file_name = file_name
        self.server_socket = server_socket
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.window = window
        self.max_segment_size = max_segment_size
        self.max_payload_size = max_segment_size - PacketHandler.HEADER_LENGTH
        self.total_packets = "ALL"
        self.timeout = timeout
        self.thread_name = thread_name
        self.buffer_size = buffer_size

    def run(self):
        log.info("[%s] Generating Packets...", self.thread_name)
        packets = self.generate_packets()
        log.info("[%s] Done", self.thread_name)
        log.info("Generated %d Packets", len(packets))

        while (not self.window.empty() or
               next(self.window) < self.total_packets):

            if self.window.full():
                pass
            elif (not self.window.full() and
                  next(self.window) >= self.total_packets):
                pass

            else:
                packet = packets[next(self.window)]
                self.window.consume(packet.SequenceNumber)
                thread_name = "SEQ=(" + str(packet.SequenceNumber) + ")"
                single_packet = solo_packet(
                    self.server_socket,
                    self.server_port,
                    self.client_ip,
                    self.client_port,
                    self.window,
                    packet,
                    self.timeout,
                    thread_name=thread_name)

                single_packet.start()

        log.info("[THREAD] [%s] Stopping Packet Transmission", self.thread_name)
        self.window.stop_transmission()

    def close(self):
        try:
            if self.server_socket:
                self.server_socket.close()
        except Exception as e:
            log.exception("Unable to Close UDP Socket")
            log.debug(e)
            raise SocketException("Unable to Close UDP Socket (%s:%d)"
                                  % (self.server_ip, self.server_port))

    def generate_packets(self):
        packets = []

        with open(self.file_name, "rb") as f:
            i = 0

            while True:
                data = f.read(self.max_payload_size)
                if not data:
                    break

                sequenceNumber = i % self.window.maxSequenceNumber()
                p = PacketHandler.PACKET(SequenceNumber=sequenceNumber, Checksum=self.checksum(data), Data=data)
                packets.append(p)
                i += 1

        if self.total_packets == "ALL":
            self.total_packets = len(packets)
        else:
            if int(self.total_packets) <= len(packets):
                self.total_packets = int(self.total_packets)
            else:
                self.total_packets = len(packets)

        return packets[:self.total_packets]

    def checksum(self, data):
        data = data.decode('utf-8')
        if (len(data) % 2) != 0:
            data += "0"

        s = 0

        for i in range(0, len(data), 2):
            data_segment = ord(data[i]) + (ord(data[i + 1]) << 8)
            s += data_segment
            s = (s & 0xffff) + (s >> 16)

        return ~s & 0xffff


def alter_bits(packet, alterations=5):
    error = random.getrandbits(8)
    altered_data = list(packet.Data)

    for i in range(alterations):
        random_byte = random.randint(0, len(altered_data))
        alteredByte = altered_data[random_byte] & error

        altered_data[random_byte] = alteredByte

    altered_data = "".join([chr(i) for i in altered_data]).encode()
    corrupted_packet = PacketHandler.PACKET(SequenceNumber=packet.SequenceNumber,
                                            Checksum=packet.Checksum,
                                            Data=altered_data)
    return corrupted_packet


def make_packet(packet):
    seq_num = struct.pack('=I', packet.SequenceNumber)
    checksum = struct.pack('=H', packet.Checksum)
    p = seq_num + checksum + packet.Data
    return p


class solo_packet(Thread):
    def __init__(self,
                 server_socket,
                 server_port,
                 client_ip,
                 client_port,
                 window,
                 packet,
                 timeout=10,
                 thread_name="Packet(?)"):
        Thread.__init__(self)
        self.server_socket = server_socket
        self.server_port = server_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.window = window
        self.packet = packet
        self.timeout = timeout
        self.thread_name = thread_name

    def run(self):
        self.rdt_send(self.packet)
        self.window.start(self.packet.SequenceNumber)

        log.info(f"{color.OKGREEN}{color.BOLD}[SEQ={self.packet.SequenceNumber}] Packet Sent {color.END}"
                f"{color.OKBLUE}({self.server_port}) ➞ ({self.client_port}){color.END}")

        while self.window.unacked(self.packet.SequenceNumber):
            timeLapsed = (time.time() -
                          self.window.start_time(self.packet.SequenceNumber))

            if timeLapsed > self.timeout:
                log.info(f"{color.HEADER}{color.BOLD}[SEQ=%d]{color.END} "
                         f"{color.OKBLUE}{color.BOLD}[RETRANSMISSION]{color.END} "
                         f"{color.OKBLUE}Resending Packet{color.END}",
                         self.packet.SequenceNumber)
                self.rdt_send(self.packet)
                self.window.restart(self.packet.SequenceNumber)

        with LOCK:
            self.window.stop(self.packet.SequenceNumber)

    def rdt_send(self, packet):
        if self.file_corruption_simulation():
            print(f"{color.FAIL}{color.BOLD}[SIMULATION] Simulating Packet Corruption for "
                  f"[ACK={packet.SequenceNumber}]{color.END}", file=sys.stderr)
            packet = alter_bits(packet)

        p = make_packet(packet)

        self.udt_send(p)

    def file_corruption_simulation(self):
        if SIMULATE_FILE_CORRUPTION:
            r = random.randint(1, 100)

            if r <= SIMULATE_FILE_CORRUPTION_PCT * 100:
                return True
            else:
                return False
        return False

    def udt_send(self, packet):
        try:
            with LOCK:
                self.server_socket.sendto(packet, (self.client_ip, self.client_port))
        except Exception as e:
            log.debug(e)
            raise SocketException("UDP Packet Transmission Failed (%s:%d)" % (self.client_ip, self.client_port))


def corrupt(received_ack):
    hashcode = hashlib.md5()
    hashcode.update(str(received_ack.AckNumber).encode())

    if hashcode.digest() != received_ack.Checksum:
        return True
    else:
        return False


def parse(received_ack):
    ack_number = struct.unpack('=I', received_ack[0:4])[0]
    checksum = struct.unpack('=16s', received_ack[4:])[0]

    ack = ACKHandler.ACK(ack_number, checksum)

    return ack


def simulate_ack_loss():
    if SIMULATE_ACK_LOSS:
        r = random.randint(1, 100)

        if r <= SIMULATE_ACK_LOSS_PCT * 100:
            return True
        else:
            return False
    return False


class ACKHandler(Thread):
    ACK = namedtuple("ACK", ["AckNumber", "Checksum"])

    def __init__(self,
                 server_socket,
                 server_ip,
                 server_port,
                 client_ip,
                 client_port,
                 window,
                 timeout=10,
                 thread_name="ACKHandler",
                 buffer_size=2048):
        Thread.__init__(self)
        self.server_socket = server_socket
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.window = window
        self.timeout = timeout
        self.thread_name = thread_name
        self.buffer_size = buffer_size

    def run(self):
        global received_ack
        while self.window.transmit():

            if self.window.empty():
                continue

            ready = select.select([self.server_socket], [], [], self.timeout)

            if not ready[0]:
                continue

            try:
                received_ack, client_ip = self.server_socket.recvfrom(self.buffer_size)
            except Exception as e:
                log.exception(f"{color.FAIL}{color.BOLD}[ACK=%d] ACK Packet Transmission Error{color.END}"
                              , received_ack.AckNumber)
                log.debug(e)
                raise SocketException("ACK Packet Transmission Error")

            if client_ip[0] != self.client_ip:
                continue

            received_ack = parse(received_ack)

            if corrupt(received_ack):
                log.critical(f"{color.FAIL}{color.BOLD}[%s] [ACK=%d] ACK Checksum Invalid{color.END}"
                             , self.thread_name, received_ack.AckNumber)
                continue

            if not self.window.exist(received_ack.AckNumber):
                log.critical("[%s] [ACK=%d] ACK Transmission Window Timeout", self.thread_name, received_ack.AckNumber)
                continue

            if simulate_ack_loss():
                print(
                    f"{color.FAIL}{color.BOLD}[SIMULATION] Simulating ACK Loss for [ACK={received_ack.AckNumber}]{color.END}"
                    , file=sys.stderr)
                log.error(f"{color.FAIL}{color.BOLD}[%s] [ACK=%d]{color.END} "
                          f"{color.FAIL}ACK Lost{color.END}", self.thread_name, received_ack.AckNumber)
                continue

            log.info(f"{color.HEADER}{color.BOLD}[ACK={received_ack.AckNumber}]{color.END} "
                     f"{color.OKBLUE}{color.BOLD}[ACK Received] "
                     f"{color.OKGREEN}Checksum OK{color.END}")
            self.window.make_acked(received_ack.AckNumber)
