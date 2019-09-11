
class M80Error(Exception):
    def __init__(self,msg=''):
        self.message = msg

class TagExistsError(M80Error):
    pass

class TagDoesNotExistError(M80Error):
    pass

class TagInvalidError(M80Error):
    pass

class FreezableNameInvalidError(M80Error):
    pass

class UnsavedChangesInThawedError(M80Error):
    pass

class NotLoggedInError(M80Error):
    pass
