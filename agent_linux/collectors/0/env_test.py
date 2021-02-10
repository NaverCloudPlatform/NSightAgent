import json
import os
import sys

sys.path.append(sys.argv[1])

from error_report import ErrorReport


def main(argv):
    err_report = ErrorReport()
    enabled = err_report.get_report_enabled()
    if enabled:
        err = {'env_test': 'ip_and_path',
               'ip': test_ip(),
               'path': test_path()}
        err_report.record_other_err(err)
        print(json.dumps(err))
        sys.stdout.flush()
    else:
        pass


def test_ip():
    return os.popen('ip addr').read().split('\n')


def test_path():
    return os.popen('echo $PATH').read().split('\n')


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
