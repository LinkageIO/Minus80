#!/usr/bin/env python3

from .Tools import guess_type

class sqlite_dict(object):
    def __init__(self,con):
        self._con = con
        con.cursor().execute('''
            CREATE TABLE IF NOT EXISTS globals (
                key TEXT,
                val TEXT,
                type TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uniqkey ON globals(key)
        ''')


    def __call__(self,key,val=None):
        try:
            if val is not None:
                val_type = guess_type(val)
                if val_type not in ('int', 'float', 'str'):
                    raise TypeError(
                        f'val must be in [int, float, str], not {val_type}'
                    )
                self._con.cursor().execute(
                    '''
                    INSERT OR REPLACE INTO globals
                    (key, val, type)VALUES (?, ?, ?)''', (key, val, val_type)
                )
            else:
                (valtype, value) = self._con.cursor().execute(
                    '''SELECT type, val FROM globals WHERE key = ?''', (key, )
                ).fetchone()
                if valtype == 'int':
                    return int(value)
                elif valtype == 'float':
                    return float(value)
                elif valtype == 'str':
                    return str(value)
        except TypeError:
            raise ValueError('{} not in database'.format(key))

    def __contains__(self,key):
        (num,) = self._con.cursor().execute(
            'SELECT COUNT(key) FROM globals WHERE key = ?', (key,)
        ).fetchone()
        if num == 0:
            return False
        elif num == 1:
            return True

    def keys(self):
        all_keys = self._con.cursor().execute('SELECT key from globals')
        return [x for x, in all_keys ]

    def __getitem__(self,key):
        return self(key)

    def __setitem__(self,key,val):
        self(key,val=val)

    def __delitem__(self,key):
        self._con.cursor().execute(
            'DELETE FROM globals WHERE key = ?',(key,)
        )

