
class ToolChainSettings:

    def __init__(self, toolchainID, pr_flag=True, commit_flag=False):
        self._pr_flag = pr_flag
        self._commit_flag = commit_flag
        self._toolchainID = toolchainID
        """
        To-Do fetch pr_flag and commit_flag values from toolchain record
        in ES that was set during the add/update process. If not found
        use defaults
        """

    """Define '_pr_flag' properties."""
    @property
    def pr_flag(self):
        return self._pr_flag

    @pr_flag.setter
    def pr_flag(self, value):
        self._pr_flag = value
        #  To-Do Set pr_flag in ES

    """Define '_commit_flag' properties."""
    @property
    def commit_flag(self):
        return self._commit_flag

    @commit_flag.setter
    def commit_flag(self, value):
        self._commit_flag = value
         #  To-Do Set commit_flag in ES

    """Define '_toolchainID' properties."""
    @property
    def toolchainID(self):
        return self._toolchainID

    @toolchainID.setter
    def toolchainID(self, value):
        self._toolchainID = value
