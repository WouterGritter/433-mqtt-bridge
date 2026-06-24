import subprocess
import threading
import time
from typing import Optional

from . import events
from . import registry
from . import stats
from .notifications import send_discord_message
from .packet import parse_rtl_433_packet


class Receiver:
    def __init__(self, name: str, arguments: str):
        self.name = name
        self.arguments = arguments
        # The currently running rtl_433 subprocess, exposed so it can be restarted from
        # the dashboard (terminating it makes receiver_worker respawn it).
        self.process: Optional[subprocess.Popen] = None

    def restart(self) -> bool:
        """Terminate the running rtl_433 process; receiver_worker respawns it. Returns
        whether there was a process to terminate."""
        process = self.process
        if process is None or process.poll() is not None:
            return False
        process.terminate()
        return True

    def start(self):
        command = f'rtl_433 {self.arguments}'

        if '-F json' not in command:
            command += ' -F json'

        if '-C si' not in command:
            command += ' -C si'

        for custom_decoder in registry.custom_decoders:
            command += f' -X {custom_decoder}'

        command_args = [arg.strip() for arg in command.split(' ') if arg.strip() != '']

        threading.Thread(target=self.receiver_worker, args=(command_args,)).start()

    def receiver_worker(self, command_args: list[str]):
        while True:
            print(f'Running rtl_433[{self.name}] with arguments {" ".join(command_args[1:])}')
            process = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.process = process
            stats.set_receiver_running(self.name, True)
            events.emit('receiver_status', stats.receiver_snapshot(self.name))

            stderr_worker_thread = threading.Thread(target=self.read_stderr_worker, args=(process,))
            stdout_worker_thread = threading.Thread(target=self.read_stdout_worker, args=(process,))

            stderr_worker_thread.start()
            stdout_worker_thread.start()

            stderr_worker_thread.join()
            stdout_worker_thread.join()

            exit_code = process.wait()
            self.process = None
            stats.set_receiver_running(self.name, False)
            stats.mark_receiver_restart(self.name)
            events.emit('receiver_status', stats.receiver_snapshot(self.name))

            message = f'rtl_433[{self.name}] exited with code {exit_code}. Restarting rtl_433 command after a delay.'
            send_discord_message(message)
            print(message)

            time.sleep(30.0)

    def read_stderr_worker(self, process: subprocess.Popen):
        while True:
            line = process.stderr.readline()
            if not line:
                break

            print(f'rtl_433[{self.name}]: {line.strip()}')

    def read_stdout_worker(self, process: subprocess.Popen):
        print(f'rtl_433[{self.name}] is now reading packets.')
        received_first = False

        while True:
            line = process.stdout.readline()
            if not line:
                break

            packet = parse_rtl_433_packet(line, self)
            if packet is None:
                print(f'Error while parsing packet on receiver rtl_433[{self.name}]: {line.strip()}')
                continue

            if not received_first:
                received_first = True

                message = f'rtl_433[{self.name}] successfully received its first packet.'
                send_discord_message(message)
                print(message)

            registry.packet_receive_queue.put(packet)
