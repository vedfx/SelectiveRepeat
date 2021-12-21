from _server_functions import FileException
from _server_functions import Server
from _server_functions import SocketException
from _server_functions import WindowSizeException
from _shared_functions import *


def Server_Start():
    server_settings = read_args(workingDir + "\\server.in")

    server_ip = "127.0.0.1"
    server_port = int(server_settings['server_port'])
    window_size = int(server_settings['window_size'])
    seq_num_bits = 16
    max_segment_size = 32736

    server = Server(server_ip, server_port, seq_num_bits, window_size, max_segment_size)

    try:
        server.open()
        server.close()

    except SocketException as e:
        print(e)
    except FileException as e:
        print(e)
    except WindowSizeException as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        server.close()


if __name__ == "__main__":
    Server_Start()
