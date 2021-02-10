import ConfigParser
import os


def get_configs():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

    config_dir = os.path.abspath(os.path.join(base_dir, 'configs', 'configs.cfg'))
    cp = ConfigParser.ConfigParser()
    cp.read(config_dir)

    env = cp.get('general', 'env')

    configs = {
        '--base-dir': base_dir,
        '--collector-dir': os.path.join(base_dir, 'collectors'),
        '--perf-collector-addr': cp.get(env, 'perf.addr'),
        '--wai-addr': cp.get(env, 'wai.addr'),
        '--wai-token': cp.get(env, 'wai.token'),
        '--sender-thread-number': cp.get(env, 'sender.threads.num'),
        '--send-package-wait': cp.get(env, 'sender.package.wait'),
        '--error-report-cycle': cp.get(env, 'err.report.cycle'),
        '--not-work-threshold': cp.get(env, 'not.work.threshold')
    }
    return configs
