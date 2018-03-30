from Tribler.Core.Config.tribler_config import TriblerConfig


class GumbyTriblerConfig(TriblerConfig):
    """
    This class is an extended version of the TriblerConfig.
    A subclass is necessary since we need a few settings that are only used in Gumby experiments.
    """

    def __init__(self, config=None):
        super(GumbyTriblerConfig, self).__init__(config=config)

        self.config['trustchain']['enabled'] = False  # Defaults to false

        self.config['triblerchain'] = {}
        self.config['triblerchain']['enabled'] = False  # Defaults to false

    def set_trustchain_enabled(self, enabled):
        self.config['trustchain']['enabled'] = enabled

    def get_trustchain_enabled(self):
        return self.config['trustchain']['enabled']

    def set_triblerchain_enabled(self, enabled):
        self.config['triblerchain']['enabled'] = enabled

    def get_triblerchain_enabled(self):
        return self.config['triblerchain']['enabled']
