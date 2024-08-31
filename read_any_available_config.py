import os

class Config(metaclass=Singleton):
    def __init__(self):
        self.config = ConfigParser(interpolation=ExtendedInterpolation())
        
        # List of possible config file names
        config_files = ['config.cfg', 'config_uat.cfg', 'config_prod.cfg']
        
        # Find the first existing config file
        config_file = next((file for file in config_files if os.path.exists(file)), None)
        
        if config_file is None:
            raise FileNotFoundError("No configuration file found")
        
        with open(config_file, "r") as fp:
            self.config.read_file(fp)
        
        # Rest of the initialization code remains the same
        Path("./build/logs").mkdir(parents=True, exist_ok=True)
        # ... (rest of the existing code)
