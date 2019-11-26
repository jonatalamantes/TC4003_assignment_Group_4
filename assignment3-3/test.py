#!/usr/bin/python
"""
Main script
"""


import argparse
import os
import sys
import subprocess

def output_bash(cmd):
    try:
        ps = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = ps.communicate()[0]
        return output
    except KeyboardInterrupt:
        return "KeyboardInterrupt: FAIL"

def main():

    current = os.getcwd()
    os.environ['GOPATH'] = current
    os.chdir(current + "/src/raft")

    parser = argparse.ArgumentParser(description='Run golang test')

    parser.add_argument('--count', type=int, default=1, help='Many times of execute the test suite')
    parser.add_argument('--debug', action="store_true", help='Debug Flag')
    parser.add_argument('--test_type', default="Election", help='Test type, default Election')
    parser.add_argument('--kv', action="store_true", help='Use kv store')
    parser.add_argument('--native', action="store_true", help='use no native flag')
    args = parser.parse_args()

    good = 1
    total = 1
    for i in range(0, args.count):
        print(" *** Start Test Suite Iteration {0} ***".format(int(i+1)))

        if args.kv:
            os.chdir(current + "/src/kvraft")

        if not args.native:

            command = "cat test_test.go | grep 'func Test' | tr '(' ' ' | awk '{print $2}'"
            if args.test_type != "all":
                command += " | grep {0}".format(args.test_type)
            test_suite = output_bash(command).split("\n")
            test_suite = sorted(list(filter(lambda x: x != "", test_suite)))
            total += len(test_suite)

            for test_cmd in test_suite:
                print("### Testing {0} ###".format(test_cmd))
                command = "go test -run {0} 2>&1 | tee {1}/execution_{0}.log ".format(test_cmd, current)

                if not args.debug:
                    command += " | grep 'FAIL\|Pass\|PASS\|ok.*raft\|Test: \|    .*.go:.*: '"

                if args.debug:
                    rc = os.system(command)
                    if rc == 0:
                        good += 1

                else:
                    out = output_bash(command)
                    print(out.strip())
                    if out.find("FAIL") == -1:
                        good += 1

                    if out.find("KeyboardInterrupt") != -1:
                        print("\nTest Results: {0}/{1} = {2}".format(good, total, float(good)/float(total)))
                        sys.exit(0)

        else:
            if args.test_type == "all":
                command = "go test 2>&1 | tee {0}/execution.log ".format(current)
            else:
                command = "go test -run {0} 2>&1 | tee {1}/execution.log ".format(args.test_type, current)

            if not args.debug:
                command += " | grep 'FAIL\|Pass\|PASS\|ok.*raft\|Test: \|    .*.go:.*: '"

            rc = os.system(command)
            if rc == 0:
                good += 1
            total += 1

    print("\nTest Results: {0}/{1} = {2}".format(good, total, float(good)/float(total)))

main()

