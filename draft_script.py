import itertools
import numpy as np
import random

class Memory:
    """The memory has fucking features I guess"""
    
    def __init__(self, size, bandwith, latency):
        self.size = size
        self.bandwith = bandwith
        self.latency

class Disk:
    """The Disk has features too"""
    
    def __init__(self, size, bandwith, latency):
        self.size = size
        self.bandwith = bandwith
        self.latency
        
# class Calculation:
    
#     def __init__(self, size):
#         self.size = size
        
    
    
class Node:
    """A Node in a distributed System"""
    newid = itertools.count()
    
    # I don't have default values
    def __init__(self, dist = 'Norm', memory_size, memory_bandwith, memory_latency):
        
        # A unique ID for the node, maybe useful later
        self.id = next(Node.newid)
        self.in_calculation = False
        # I don't know if we need this
        self.dependent_nodes = []
        self.target_nodes = []
        
        # Distribution of the nodes is random among Uniform or Normal
        if dist == "Unif":
            self.x = np.random.uniform()
            self.y = np.random.uniform()
        else:
            self.x = np.random.normal()
            self.y = np.random.normal()
        
        # Role of Node, Worker or Master for Map Reduce, type commodity or server
        self.role = 'Worker'
        self.type = 'Commodity'
        
        # Features of Node (I don't know how these affect the computation time)
        
        self.memory = Memory(memory_size, memory_bandwith, memory_latency)
        self.disk = Disk(memory_size, memory_bandwith, memory_latency)
        self.network = Network(network_bandwith)
        
        # maybe there should be core and processor object
        self.processors = p
        self.cores = c
        self.failure_rate = f
        
        
        # Values we accumulate at the end
        self.time_in_computation = 0
        self.time_in_waiting = 0
        self.time_in_communication = 0
        self.time_in_failure = 0
        self.memory_access = 0
    
    # this is an alternative to the claculate function, instead of adding this function will count along with an external clock
    # and accumulate values with that clock
    def run():
        if self.in_calculation:
            self.calculation_time -= 1
        else: 
            pass
            #request_computation()
            
    
    # Maybe we will not use a calculate function of a funciton that runs every unit.
    def calculate():
        # We need to decide how attributes affect the calculation time. 
        # One thing that I think we should do is only allow the calculate function
        # to be called when self.
        if self.failure_rate < random.uniform(0,1):
            self.time_in_failure += 1;
        else:
            if self.computation == 'grep': 
                self.time_in_computation += 1;
            elif self.computation == 'search':
                self.time_in_computation += 1;
        
    def communicate(dest_node):
        # Distance between nodes and some kind of bandwith will be used
        # to calculate the time in communication 
        dest_node.time_in_waiting += 1
        self.time_in_communication += 1
    
        
def generate_nodes(n=20, dist = "Norm"):
    # Returns a list of node objects
    return []

def collect_node_stats(node_list):
    return {time_in_computation:0,
            time_in_waiting:0,
            time_in_failure:0,
            memory_access:0l