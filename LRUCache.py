#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from collections import OrderedDict

class LRUCache(OrderedDict):
    """
    #实现一个LRU算法的dict，用于记录order_id
    #避免storm平台重发log记录，造成redis数据偏大！
    """
    def __init__(self, capacity):
        """
        @param capacity 字典长度
        """
        self.capacity = capacity
        self.cache = OrderedDict()
    
    def get(self,key):
        if self.cache.has_key(key):
            value = self.cache.pop(key)
            self.cache[key] = value
        else:
            value = None
        return value
    
    def set(self,key,value):
        if self.cache.has_key(key):
            value = self.cache.pop(key)
            self.cache[key] = value
        else:
            if len(self.cache) == self.capacity:
                 #pop出第一个item
                self.cache.popitem(last = False)
                self.cache[key] = value
            else:
                self.cache[key] = value


if __name__ == "__main__":
    sys.exit(0)
    c = LRUCache(3)

    c.set('t1', 1)
    c.set('t2', 2)
    c.set('t3', 3)

    print c.cache

    c.set('t4', 4)
    print c.cache

    print c.get("t1")
    print c.get("t2")