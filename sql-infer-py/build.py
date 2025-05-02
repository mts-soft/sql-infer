from glob import glob
import os
from subprocess import Popen
import sys


def main() -> None:
    exit_code = Popen(["sh", "./build.sh"]).wait()
    if exit_code != 0:
        print("Build failure.")
        sys.exit(-1)
    for file in glob("builds/sql-infer*"):
        os.system(f"cp {file} ./sql-infer-py/bin")
    os.system("cd sql-infer-py && poetry build -o dist")


if __name__ == "__main__":
    main()
