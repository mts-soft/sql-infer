def run():
    import os
    import subprocess
    import sys
    import platform
    bin_name = "sql-infer-linux"
    if platform.system() == "Windows":
        bin_name = "sql-infer-win.exe"
    elif platform.system() == "Darwin":
        bin_name = "sql-infer-macos"
    bin_dir = os.path.join(os.path.dirname(__file__), "bin")
    binary = os.path.join(bin_dir, bin_name)
    if not os.path.exists(binary):
        print(f"Sql Infer binary not found in {bin_dir} {bin_name}", file=sys.stderr)
        sys.exit(1)
    sys.exit(subprocess.call([binary] + sys.argv[1:]))
