class FileExistsAndShouldBeSkippedError(Exception):
    pass


class FileExistsAndIsIdenticalError(FileExistsAndShouldBeSkippedError):
    pass


class FileExistsAndCannotBeComparedError(FileExistsAndShouldBeSkippedError):
    pass

class AlreadyUnderEvaluationError(Exception):
    """This Task is already being processed by a different worker"""
    pass
