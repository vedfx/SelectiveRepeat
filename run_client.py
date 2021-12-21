import os

from _client_functions import Client
from _client_functions import FileException
from _client_functions import SocketException
from _client_functions import WindowSizeException
from _shared_functions import *


def Client_Start():
    client_settings = read_args(workingDir + "\\client.in")

    file_name = client_settings['file_name']
    server_ip = client_settings['server_ip']
    server_port = int(client_settings['server_port'])
    client_ip = "127.0.0.1"
    client_port = int(client_settings['client_port'])
    window_size = int(client_settings['window_size'])
    seq_num_bits = 16
    timeout = 10
    client_data_folder = os.path.join(os.getcwd(), "client")

    client = Client(client_ip,
                    client_port, server_ip,
                    server_port,
                    seq_num_bits,
                    window_size,
                    client_data_folder)
    try:
        client.receive(file_name,
                       server_ip,
                       server_port,
                       client_ip, client_port,
                       timeout)

        client.close()

    except SocketException as e:
        print(e)
    except FileException as e:
        print(e)
    except WindowSizeException as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        client.close()


if __name__ == "__main__":
    Client_Start()
