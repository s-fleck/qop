class FileExistsAndIsIdenticalError(Exception):
    pass


class AlreadyUnderEvaluationError(Exception):
    """This Task is already being processed by a different worker"""
    pass
