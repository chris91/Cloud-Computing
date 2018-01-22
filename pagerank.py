# Author: Christos Aleiferis
# 

from mrjob.job import MRJob
from mrjob.step import MRStep
import re


WORD_RE = re.compile(r"[0-9]+")


class MRWordFrequencyCount(MRJob):
    # the values the reducer receives are sorted
    SORT_VALUES = True
    
    def read_file(self, _, line):
        # read the file and initialize the adjacency list (dictionary)
        nodes = WORD_RE.findall(line)
        node2_dict = {}
        # eliminate lines which do not contain edges from node to node
        if(len(nodes) == 2):
            node1 = nodes[0]
            node2_dict.update({nodes[1]:1})
            yield node1, node2_dict

    
    def create_adjacency_list(self, node1, node2_dict):
        # create the adjacency list (dictionary)
        adjacency_dict= {}
        for node2 in node2_dict:
            for k, v in node2.items():
                adjacency_dict.update({k:v})
                
        yield node1, adjacency_dict

    def init_pagerank(self, node1, node2_dict):
        # implement the mapper for pagerank
        # initialize the pagerank
        pagerank = 1 / len(node2_dict)
        adjacency_dict = {}

        adjacency_dict = {node2:pagerank for node2 in node2_dict}

        # pass along the graph structure
        yield node1, adjacency_dict
        # update the nodes linked from node1
        for node2, pr in adjacency_dict.items():
            yield node2, pr

        
    def propagate_pagerank(self, node, pr_or_dict):
        # recreate the adjavency dictionary and update the pagerank values in it
        recreated_adjacency_dict = {}
        new_pagerank = 0
        for i in pr_or_dict:
            # check if the attributes are from yield node1, adjacency_dict
            if type(i) is dict:
                recreated_adjacency_dict = {k:v for k, v in i.items()}

            elif type(i) is float:
                new_pagerank += i

        for k, v in recreated_adjacency_dict.items():
            recreated_adjacency_dict.update({k:new_pagerank})
            
        yield None, recreated_adjacency_dict
        
    def ten_highest_pagerank(self, _, recreated_adjacency_dict):
        # return the top ten nodes that have received the largest pageranks
        pr_dict = {}
        for i in recreated_adjacency_dict:
            for k, v in i.items():
                pr_dict.update({k:v})
        
        top_ten_list = sorted(pr_dict, key = pr_dict.__getitem__ , reverse = True)[:10]
        yield _, top_ten_list 
         
        
    def steps(self):
        return [
            MRStep(mapper=self.read_file,
                   reducer=self.create_adjacency_list),
            MRStep(mapper=self.init_pagerank,
                   reducer=self.propagate_pagerank),
            MRStep(reducer=self.ten_highest_pagerank)
        ]
    

if __name__ == '__main__':
    MRWordFrequencyCount.run()
