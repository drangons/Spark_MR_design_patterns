# -*- coding: utf-8 -*-
"""
Created on Tue Jul 28 19:09:16 2015

@author: dikshith
"""
import pyhashxx
import hashlib
import mmh3
import math
from bitarray import bitarray
 
class BloomFilter():
    """
    Implements the boom filter
    """
    
    def __init__(self,n,e=0.01):
        self.n = n
        self.e = e
        self.m = self._get_bloomfilter_size(n,e)
        self.k = self._get_optimalk(n,self.m)
        self.filter = bitarray(self.m)
        self.filter.setall(0)
        
    def _get_bloomfilter_size(self,n,e):
        """
        Calculates the bloom filter size
        Input:  n size of the input
                e false positive rate
        Output: m the size of the boom filter
        """
        return  int((-1 * n * math.log(e)) // (math.pow(math.log(2),2)))

    def _get_optimalk(self,n,m):
        """
        Calculates the number of hash functions to use
        Input:  n size of the input
                m the size of bloom filter
        Output: k the number of hash functions to use   
        """
        return int((m * math.log(2)) // n)
        
    def add(self,data):
        """
        Trains the bloom filter
        Input: data the string value
        """
        #md5 expects bytecode in new version
        en = data.encode()
        #TODO: implement different hash function with random seed ?
        for seed in range(self.k):
            result = mmh3.hash(en,seed) % self.m
            self.filter[result] = 1
        
    def test(self,data):
        """
        Test whether the string is memeber of the set
        Output: True if part of set
                False otherwise
                
        The true value should be interpreted as probably, as there may be 
        false positives due to hash collision
        """
        en = data.encode()
        for seed in range(self.k):
            result = mmh3.hash(en,seed) % self.m
            if self.filter[result] == 0:
                return False
        return True 
    
    def __contains__(self,key):
        """
        Synatatic sugar for test function
        """
        return self.test(key)
        
def main():
    bf = BloomFilter(10000,0.01)
    
    with open('sample.txt','r') as f:
        for line in f.readlines():
            #TODO: preprocess the lines 
            for word in line.split(" "):
                bf.add(word)
            
    
    print(bf.test("Bloom")) # should be intrepreted as maybe,
    #in the post processing we can perform the remove false positives
    print("Bloom" in bf)
    
    print(bf.test("hello"))
    
    
if __name__ == '__main__':
    main()
    

