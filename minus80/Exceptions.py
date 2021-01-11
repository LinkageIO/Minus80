class M80Error(Exception):
    def __init__(self, msg=""):
        self.message = msg


# Minus 80 related Exceptions
class FreezableNameInvalidError(M80Error):
    pass


class UnsavedChangesInThawedError(M80Error):
    def __init__(self, msg="", new=None, changed=None, deleted=None):
        super().__init__(msg)
        self.new = new
        self.changed = changed
        self.deleted = deleted


class DatasetDoesNotExistError(M80Error):
    pass


# Tag Exceptions
class TagExistsError(M80Error):
    pass


class TagConflictError(M80Error):
    pass


class TagDoesNotExistError(M80Error):
    pass


class TagInvalidError(M80Error):
    pass


# Cloud Exceptions
class UserNotLoggedInError(M80Error):
    pass


class UserNotVerifiedError(M80Error):
    pass


class PushFailedError(M80Error):
    pass


class PushFileFailedError(M80Error):
    pass


class CloudListFailedError(M80Error):
    pass


class CloudDatasetDoesNotExistError(M80Error):
    pass


class CloudTagDoesNotExistError(M80Error):
    pass


class CloudPullFailedError(M80Error):
    pass
