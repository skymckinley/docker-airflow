#!/usr/bin/env python

# HSU ETL utitility class
import hashlib

class HSU_ETL:

    def __hash_preprocess(self, value):
        value_type = type(value)

        typemap = {
            'date': 'date',
            'str': value,
            'number': 'number',
            'integer': 'integer',
            'decimal': 'decimal',
            'long': 'long',
            'boolean': 'boolean',
            'byte': 'byte',
            'cbyte': 'cybte'
                }
        return(typemap.get(value_type, '-'))

    def hash(self, *args):
        hash_string = ''.join(str(args))

        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
