
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
    def __init__(self,msg='',new=None,changed=None,deleted=None):
        super().__init__(msg)
        self.new = new
        self.changed = changed
        self.deleted = deleted

class UserNotLoggedInError(M80Error):
    pass

class UserNotVerifiedError(M80Error):
    pass
