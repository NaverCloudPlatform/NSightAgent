import ConfigParser
import os


def get_configs():
    collectors_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

    config_dir = os.path.join(collectors_dir, 'configs')
    config_file_path = os.path.join(config_dir, 'config.cfg')
    config = ConfigParser.ConfigParser()
    config.read(config_file_path)

    config.add_section('base')
    config.set('base', 'collectors_dir', collectors_dir)

    return config
