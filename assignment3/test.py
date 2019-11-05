#!/usr/bin/python
"""
Main script
"""


import argparse
import os

def main():

    current = os.getcwd()
    os.environ['GOPATH'] = current
    os.chdir(current + "/src/raft")

    parser = argparse.ArgumentParser(description='Run golang test')

    parser.add_argument('--count', type=int, default=1, help='Many times of execute the test suite')
    parser.add_argument('--debug', action="store_true", help='Debug Flag')
    parser.add_argument('--test_type', default="Election", help='Test type, default Election')
    args = parser.parse_args()

    for i in range(0, args.count):
        print(" *** Start Test Suite Iteration {0} ***".format(int(i+1)))

        if args.test_type == "all":
            command = "go test 2>&1 | tee {0}/execution.log ".format(current)
        else:
            command = "go test -run {0} 2>&1 | tee {1}/execution.log ".format(args.test_type, current)

        if not args.debug:
            command += " | grep 'FAIL\|Pass\|PASS\|ok   raft    \|Test: \|    .*.go:.*: '"

        os.system(command)

main()

