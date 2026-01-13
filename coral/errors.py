class CoralError(Exception):
    pass


class ConfigError(CoralError):
    pass


class ProviderError(CoralError):
    pass


class ResolverError(CoralError):
    pass


class PackagingError(CoralError):
    pass
