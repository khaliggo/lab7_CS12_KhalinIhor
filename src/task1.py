import os
import sys
import mmap
import fcntl
import pickle
import struct
import random
import string
import tempfile

# Layout of the shared mmap region:
#   [0:4]   - uint32, number of bytes currently occupied after the header
#   [4:...] - packed messages, each prefixed with a 4-byte uint32 length

HEADER_SIZE = 4       # bytes reserved for the "used size" counter
MMAP_SIZE = 65536     # 64 KB total shared region
NUM_CHILDREN = 4


def random_string(min_len=5, max_len=30):
    length = random.randint(min_len, max_len)
    chars = string.ascii_letters + string.digits
    return ''.join(random.choices(chars, k=length))


def write_message(mem, lock_fd, message):
    """Append a pickle-serialised message to the shared mmap under flock."""
    payload = pickle.dumps(message)
    # 4-byte big-endian length prefix followed by the payload
    record = struct.pack('>I', len(payload)) + payload

    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    try:
        # Read current used-bytes offset from header
        mem.seek(0)
        used = struct.unpack('>I', mem.read(HEADER_SIZE))[0]

        end = HEADER_SIZE + used + len(record)
        if end > MMAP_SIZE:
            print(
                f"Child {os.getpid()}: ERROR - not enough space",
                file=sys.stderr,
            )
            return

        # Write the record at the current end of used space
        mem.seek(HEADER_SIZE + used)
        mem.write(record)

        # Update the used-bytes counter in the header
        new_used = used + len(record)
        mem.seek(0)
        mem.write(struct.pack('>I', new_used))
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)


def child_worker(mem, lock_path):
    """Code that runs in each child process."""
    pid = os.getpid()
    message = {
        'pid': pid,
        'str': random_string(),
    }
    print(f"\tChild {pid}: sending message: {message}")

    lock_fd = os.open(lock_path, os.O_RDWR)
    try:
        write_message(mem, lock_fd, message)
    finally:
        os.close(lock_fd)

    mem.close()
    os._exit(0)


def read_all_messages(mem):
    """Read all messages written by the children from the shared mmap."""
    mem.seek(0)
    used = struct.unpack('>I', mem.read(HEADER_SIZE))[0]

    messages = []
    pos = 0
    while pos < used:
        mem.seek(HEADER_SIZE + pos)
        raw_len = mem.read(4)
        if len(raw_len) < 4:
            break
        payload_len = struct.unpack('>I', raw_len)[0]
        payload = mem.read(payload_len)
        if len(payload) < payload_len:
            break
        messages.append(pickle.loads(payload))
        pos += 4 + payload_len

    return messages


def _main():
    # Create the empty lock file (used only for flock coordination)
    lock_fd, lock_path = tempfile.mkstemp(prefix='mmap_lock_')
    os.close(lock_fd)
    print(f"Parent ({os.getpid()}): lock file created at '{lock_path}'")

    # Create the anonymous shared mmap
    mem = mmap.mmap(-1, MMAP_SIZE)

    # Initialise the header: 0 bytes used
    mem.seek(0)
    mem.write(struct.pack('>I', 0))

    # Fork children
    child_pids = []
    for _ in range(NUM_CHILDREN):
        pid = os.fork()
        if pid == 0:
            child_worker(mem, lock_path)
            # child_worker calls os._exit; unreachable
        else:
            child_pids.append(pid)

    # Parent: wait for all children
    for cpid in child_pids:
        waited_pid, status = os.waitpid(cpid, 0)
        if os.WIFEXITED(status):
            code = os.WEXITSTATUS(status)
            print(f"Parent: child {waited_pid} exited with code {code}")
        else:
            print(f"Parent: child {waited_pid} terminated abnormally")

    # Read and display all messages from shared mmap
    print("\nParent: reading all messages from shared mmap:")
    messages = read_all_messages(mem)
    for i, msg in enumerate(messages, start=1):
        print(f"  [{i}] pid={msg['pid']}, str={msg['str']!r}")

    # Cleanup
    mem.close()
    os.unlink(lock_path)
    print(f"\nParent: lock file '{lock_path}' removed. Done.")


if __name__ == '__main__':
    _main()
