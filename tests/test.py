import random
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

import fire


def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
        )

        def stream_output(pipe, prefix):
            for line in pipe:
                with threading.Lock():
                    print(
                        f"{prefix} Thread-{threading.current_thread().name}: {line}",
                        end="",
                    )

        stdout_thread = threading.Thread(
            target=stream_output, args=(process.stdout, "OUT")
        )
        stderr_thread = threading.Thread(
            target=stream_output, args=(process.stderr, "ERR")
        )

        stdout_thread.start()
        stderr_thread.start()

        process.wait()

        stdout_thread.join()
        stderr_thread.join()

    except Exception as e:
        print(f"Error in thread {threading.current_thread().name}: {str(e)}")


def main(with_diff_queues: bool = False, n_workers: int = 50):
    queues = ["DEFAULT"] if not with_diff_queues else ["A", "B", "C"]
    cmd = "python3 -m workcraft --workcraft_path=example.workcraft --queues="

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        _ = [
            executor.submit(run_command, f'{cmd}["{random.choice(queues)}"]')
            for _ in range(n_workers)
        ]


if __name__ == "__main__":
    fire.Fire(main)
