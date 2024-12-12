#!/usr/bin/env python3


# Author: kokurewm (400551006)
# Description:  4DS3 Final Portfolio, Part 1
# Synopsis:  Priority-based task spooler demonstrating data structures and algorithms from the course being applied. Client portion.


import argparse
import math
import contextlib
import zmq
import sys


@contextlib.contextmanager
def message_queue(addr):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{addr}")

    socket.setsockopt(zmq.LINGER, 0)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    try:
        yield (socket, poller)
    finally:
        socket.close()
        context.term()


def cli(argv):
    parser = argparse.ArgumentParser(argv)
    parser.add_argument(
        "-e",
        "--endpoint",
        type=str,
        default="localhost:15411",
        help="host:port to connect to.",
    )

    subparsers = parser.add_subparsers(help="subcommand help", dest="op")

    parser_push = subparsers.add_parser(
        "add", help="add a new job by 'pushing' it on to the heap, with a set priority."
    )
    parser_push.add_argument(
        "-p", "--priority", type=int, required=True, help="priority of new job."
    )
    parser_push.add_argument(
        "-c",
        "--command",
        nargs="+",
        type=str,
        required=True,
        help="shell command to run (defines 'job').",
    )

    parser_rm = subparsers.add_parser("rm", help="delete a queued or running job.")
    parser_rm.add_argument("id", help="job id to delete.")

    parser_status = subparsers.add_parser(
        "status", help="show status of queued or running jobs."
    )

    return parser.parse_args()


def main():
    args = cli(sys.argv[1:])
    with message_queue(args.endpoint) as (mq, poller):
        match args.op:
            case "add":
                mq.send_json((" ".join(args.command), args.priority))
            case "status":
                mq.send_json((args.op, math.inf))
            case "rm":
                args.cmd = f"{args.op}:{args.id}"
                mq.send_json((args.cmd, math.inf))
            case _:
                return

        if poller.poll(5 * 1000):
            reply = mq.recv_string()
            if reply:
                print(reply)
        else:
            print(f"failed to connect to '{args.endpoint}', quitting.", file=sys.stderr)


if __name__ == "__main__":
    main()
