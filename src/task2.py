import os
import sys
import mmap
import fcntl
import pickle
import time

# Fixed slot layout in the mmap file:
#   The file is divided into NUM_CHILDREN equally-sized slots.
#   Each slot holds exactly SLOT_SIZE bytes.
#   Slot N starts at byte offset: N * SLOT_SIZE
#
#   Within each slot:
#     [0:4]        - uint32 big-endian: actual payload length (0 = empty)
#     [4:SLOT_SIZE] - pickle payload, zero-padded to fill the rest

NUM_CHILDREN = 3
SLOT_SIZE = 256          # bytes per slot (big enough for any pickle message)
FILE_SIZE = NUM_CHILDREN * SLOT_SIZE
FILE_NAME = 'mmap_slots.bin'
READ_INTERVAL = 0.2      # seconds between parent polling rounds


def slot_offset(slot_index):
    return slot_index * SLOT_SIZE


def child_write(slot_index, file_name):
    """Each child opens the file, locks its slot, writes a message, unlocks."""
    pid = os.getpid()
    message = {
        'pid': pid,
        'slot': slot_index,
        'text': f'Hello from child {pid} in slot {slot_index}',
    }
    payload = pickle.dumps(message)
    if 4 + len(payload) > SLOT_SIZE:
        print(
            f"\tChild {pid}: ERROR payload too large for slot",
            file=sys.stderr,
        )
        os._exit(1)

    fd = os.open(file_name, os.O_RDWR)
    mem = mmap.mmap(fd, 0)
    start = slot_offset(slot_index)

    print(f"\tChild {pid} (slot {slot_index}): acquiring exclusive lock")
    fcntl.lockf(fd, fcntl.LOCK_EX, SLOT_SIZE, start, os.SEEK_SET)
    try:
        print(f"\tChild {pid} (slot {slot_index}): writing message: {message}")
        # Write 4-byte length header then payload, zero-pad the rest
        length_bytes = len(payload).to_bytes(4, byteorder='big')
        record = length_bytes + payload
        padded = record + b'\x00' * (SLOT_SIZE - len(record))
        mem.seek(start)
        mem.write(padded)
        mem.flush()
    finally:
        fcntl.lockf(fd, fcntl.LOCK_UN, SLOT_SIZE, start, os.SEEK_SET)
        print(f"\tChild {pid} (slot {slot_index}): released lock")

    mem.close()
    os.close(fd)
    os._exit(0)


def read_slot(fd, mem, slot_index):
    """
    Parent reads one slot with a shared lock.
    Returns the deserialized message, or None if the slot is empty.
    """
    start = slot_offset(slot_index)
    fcntl.lockf(fd, fcntl.LOCK_SH, SLOT_SIZE, start, os.SEEK_SET)
    try:
        mem.seek(start)
        raw = mem.read(SLOT_SIZE)
    finally:
        fcntl.lockf(fd, fcntl.LOCK_UN, SLOT_SIZE, start, os.SEEK_SET)

    payload_len = int.from_bytes(raw[:4], byteorder='big')
    if payload_len == 0:
        return None
    return pickle.loads(raw[4:4 + payload_len])


def parent_poll(fd, mem, expected_count, timeout=10.0):
    """
    Periodically read all slots until all expected messages arrive
    or timeout is exceeded.
    """
    received = {}
    deadline = time.monotonic() + timeout
    while len(received) < expected_count:
        if time.monotonic() > deadline:
            print("Parent: WARNING - timeout waiting for messages")
            break
        for idx in range(NUM_CHILDREN):
            if idx in received:
                continue
            msg = read_slot(fd, mem, idx)
            if msg is not None:
                print(f"Parent: received from slot {idx}: {msg}")
                received[idx] = msg
        if len(received) < expected_count:
            time.sleep(READ_INTERVAL)
    return received


def create_mmap_file(file_name, size):
    """Create a zero-filled file of the given size."""
    fd = os.open(
        file_name,
        os.O_RDWR | os.O_CREAT | os.O_TRUNC,
        0o644,
    )
    os.write(fd, b'\x00' * size)
    os.close(fd)


def _main():
    create_mmap_file(FILE_NAME, FILE_SIZE)
    print(
        f"Parent ({os.getpid()}): created '{FILE_NAME}' "
        f"({FILE_SIZE} bytes, {NUM_CHILDREN} slots of {SLOT_SIZE} bytes each)"
    )

    fd = os.open(FILE_NAME, os.O_RDWR)
    mem = mmap.mmap(fd, 0)

    # Fork one child per slot
    child_pids = []
    for slot_index in range(NUM_CHILDREN):
        pid = os.fork()
        if pid == 0:
            # Child: close parent's fd/mem, open fresh ones
            mem.close()
            os.close(fd)
            child_write(slot_index, FILE_NAME)
            # child_write calls os._exit; unreachable
        else:
            child_pids.append(pid)

    # Parent: poll slots until all messages arrive
    print(f"Parent: polling {NUM_CHILDREN} slots for messages...")
    received = parent_poll(fd, mem, NUM_CHILDREN)

    # Wait for all children to finish
    for cpid in child_pids:
        waited_pid, status = os.waitpid(cpid, 0)
        if os.WIFEXITED(status):
            code = os.WEXITSTATUS(status)
            print(f"Parent: child {waited_pid} exited with code {code}")
        else:
            print(f"Parent: child {waited_pid} terminated abnormally")

    print(f"\nParent: all {len(received)} messages collected:")
    for idx in sorted(received):
        print(f"  slot {idx}: {received[idx]}")

    mem.close()
    os.close(fd)
    os.unlink(FILE_NAME)
    print(f"\nParent: '{FILE_NAME}' removed. Done.")


if __name__ == '__main__':
    _main()
