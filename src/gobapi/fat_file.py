import os

from flask import send_file


def fat_file(gbs):
    try:
        gbs = int(gbs)
        if not 1 <= gbs <= 25:
            raise ValueError
    except ValueError:
        return "", 204
    gb = 1024*1024*1024
    filename = "/tmp/largefile"
    print("File write...")
    with open(filename, 'wb') as f:
        while gbs > 0:
            print("Write GB...")
            f.write(os.urandom(gb))
            gbs -= 1
    print("File written", os.path.getsize(filename))
    return send_file(filename)
