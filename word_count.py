# Author: Christos Aleiferis
# 

from mrjob.job import MRJob
from mrjob.step import MRStep
import re


WORD_RE = re.compile(r"[a-zA-Z]+")


class MRWordFrequencyCount(MRJob):
    # the values the reducer receives are sorted
    SORT_VALUES = True
    
    def mapper(self, _, line):
        words_array = WORD_RE.findall(line)

        for word1 in words_array:
            word_dict = {}
            # to not get out of bounds of line
            if words_array.index(word1)+1 < len(words_array):
                for word2 in words_array:
                    if words_array.index(word1)+1 < len(words_array):
                        if word1 == word2:
                            next_word = words_array[words_array.index(word1)+1].lower()
                            # if next_word seen before aggregate 
                            if next_word in word_dict:
                                word_dict[next_word] += 1
                        
                            # if next_word appears first time add new entry to the dictionary
                            else:
                                word_dict.update({next_word:1})

                                
            yield word1.lower(), word_dict
        
    def reducer(self, word, stripes):
        new_dict = {}
    
        for i in stripes:
            # for every stripe aggregate the values
            for k, v in i.items():
                new_dict[k] = new_dict.get(k, 0) + v
        
        yield word, new_dict
        
    def relative_freq(self, word, stripes):
        # compute the total frequencies
        total = 0
        # dictionary associated with one word
        relative_freq = {}
        for i in stripes:
            total += sum(i.values())
            #print(i.values(),total)
            for k, v in i.items():
                relative_freq[k] = relative_freq.get(k, 0) + float(v)/float(total)
            
            #print(word,relative_freq)
        
        yield word, relative_freq
     
        
    def ten_most_pop_words(self, word, stripes):
        # in this program search 10 words most likely appeared after "my"
        # for different searches change accordingly
        top_ten = {}
        for i in stripes:
            for k, v in i.items():
                top_ten[k] = top_ten.get(k,0) + v
        
        top_ten_list = sorted(top_ten, key = top_ten.__getitem__, reverse = True)[:10]
        
        if (word == "my"):    
            print(word, top_ten_list)
     
    
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.relative_freq),
            MRStep(reducer=self.ten_most_pop_words)
        ]
    

if __name__ == '__main__':
    MRWordFrequencyCount.run()
