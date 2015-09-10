from __future__ import print_function
import Pyro4
import socket
import sys
import re
import os
#import getdata

"""
node_key: the key of the node
contents: the list of nodes present in the network
KeyValueData: the input data from the user (to store the key-value pair)

"""
Pyro4.config.REQUIRE_EXPOSE = False   
     
class Nodelist(object):
    def __init__(self):
        self.contents = {}
        self.KeyValueData = {}

    def list_contents(self):
		return self.contents
		
    def KVP(self):
		return self.KeyValueData
		
    def update_KVD(self,KVD):
		self.KeyValueData = KVD
		self.write_data()
		
    def transfer_data_cw(self, tr_node, data):
		self.KeyValueData = dict(self.KeyValueData.items() + data.items())

    def node_join(self, ip, node_num):
        self.contents[ip] = node_num
        print("{0} joined.".format(node_num))
        
    def contents_update(self,up_dict):
		self.contents = dict(self.contents.items() + up_dict.items())
		
    def contents_update_leave(self,node_del):
		del self.contents[node_del]
		print (self.contents)
		
    #def contents_remove(self,key):
		#del self.contents[key]
		#self.list_contents()
        
    def convert_utf(self,dic):
		dic_keys = dic.keys()
		for index in range(len(dic_keys)):
			new_keys = dic_keys[index].encode('utf-8')
			dic[new_keys] = dic.pop(dic_keys[index])
		self.utf_nodes = dic
        
    def contact_node(self):
		ip_to_contact = raw_input('Enter the IP and Port of the node to contact (Format -> IP:Port) -> ')
		uri='PYRO:main@' + ip_to_contact
		node_remote = Pyro4.Proxy(uri)
		try:
			self.list_of_nodes = node_remote.list_contents()
			self.convert_utf(self.list_of_nodes)
		except Exception as error_message:
			print (error_message)
			
    def update_node(self,ip,node_key):
		
		## Getting the current set of nodes and 
		## announcing its presence
		
		try:
			self_nodes = self.contents
			other_nodes = self.utf_nodes
			other_nodes_keys = self.utf_nodes.keys()
			current_nodes = dict(self_nodes.items() + other_nodes.items())
			self.contents = current_nodes
			for index in range(len(other_nodes_keys)):
				uri='PYRO:main@' + other_nodes_keys[index]
				node_remote = Pyro4.Proxy(uri)
				node_remote.contents_update(current_nodes)
			print (self.list_contents())
		except Exception as error_message:
			print (error_message)
		
    def	transfer_data(self,ip,node_key):    ## Request transfer of data	
		try:
			node_system = sorted(self.utf_nodes.values())
			print(node_system)
			if max(node_system)<node_key or min(node_system)>node_key:
				reqst_node = node_system[0] ##reqst_node: the node to request data from (clockwise neighbor)
				cc_neibor = node_system[-1]
			else:
				for node_no in node_system:
					if node_no<node_key:
						cc_neibor = node_no
					if node_no>node_key:
						reqst_node=node_no
						break
			for key,val in self.contents.iteritems():
				if val == reqst_node:
					node_addr = key
			uri = "PYRO:main@" + node_addr
			print("%s"%uri)
			remote = Pyro4.Proxy(uri)
			newKVdata = remote.KVP() ## collecting the data stored by the clockwise neighbour
			print("____",newKVdata)
			tempKVdata = newKVdata.copy()
			for key,val in newKVdata.iteritems():
				if min(node_system)>node_key:
					if key<node_key or key>cc_neibor:
						self.KeyValueData[key] = val
						del tempKVdata[key]
				else:
					if key<node_key and key>cc_neibor:
						self.KeyValueData[key] = val
						del tempKVdata[key]
			self.write_data()
			remote.update_KVD(tempKVdata)
			
		except Exception as error_message:
			print (error_message)
			
			
    def write_data(self):
		with open(file_name,'w') as fp:
				for p in self.KeyValueData.items():
					fp.write("%s : %s\n" %p)
		fp.close()
		
    def store_nodes(self,KVpair,self_addr):
		print (self.contents, KVpair)
		node_data = KVpair.keys()
		node_system = sorted(self.contents.values())
		print("**** %s *****"%node_system)
		if max(node_system)<node_data[0]:
			send_node = node_system[0]
		else:
			for node_no in node_system:
				if node_no>node_data[0]:
					send_node=node_no
					break
		for key,val in self.contents.iteritems():
			if val == send_node:
				node_addr = key
		ip_self = [(s.connect(('8.8.8.8',80)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]
		if node_addr == self_addr:
			self.KeyValueData = dict(self.KeyValueData.items() + KVpair.items())
			self.write_data()
		else: # send node to correct destination
			uri = "PYRO:main@" + node_addr
			remote = Pyro4.Proxy(uri)
			remote.store_nodes(KVpair,node_addr)
			
	## function for leaving a node
	
    def node_leave(self, xxx, item): ## xxx is the uri, item is the node address
		print ("Getting ready to leave...")
		node_no = self.contents[item]
		node_system = sorted(self.contents.values())
		leaving_node_idx = node_system.index(node_no)
		if node_no == node_system[-1]:
			cw_neibor = node_system[0]
			print(cw_neibor)
		else:
			cw_neibor = node_system[leaving_node_idx+1]
			print(cw_neibor)
		del self.contents[item] ##item is the node address
		
		#announcing its leave
		
		for index in range(len(self.contents.keys())):
			uri='PYRO:main@' + self.contents.keys()[index]
			node_remote = Pyro4.Proxy(uri)
			node_remote.contents_update_leave(item)
		
		print("%s test 1"%self.contents)
		for key,val in self.contents.iteritems():
			uri = "PYRO:main@" + key
			remote_u = Pyro4.Proxy(uri)
			#remote_u.contents_remove(key)
			if val == cw_neibor:
				transfer_node = key
				uri_t = "PYRO:main@" + transfer_node
		remote = Pyro4.Proxy(uri_t)
		data = self.KeyValueData
		remote.transfer_data_cw(transfer_node,data)
		remote.write_data() ## check this
		os.remove(file_name)
		print ("Shutting down")
		try:
			daemon_new.shutdown()
		except Exception as error_message:
			print (error_message)
		print ("Node left. Safe to shut down.")
		test=self.list_contents()
		print("%s test 2"%test)
			
    def get_val(self,requested_key):  ## getting the requested value for a key
		nodes = sorted(self.contents.values())
		if requested_key>nodes[-1]:
			key_storage_node = nodes[0]
		else:
			for node_no in nodes:
				if node_no>requested_key:
					key_storage_node = node_no
					break
		for key,val in self.contents.iteritems():
			if val == key_storage_node:
				key_storage_ip = key
		uri = "PYRO:main@" + key_storage_ip
		remote = Pyro4.Proxy(uri)
		data = remote.KVP()
		corr_value = data[requested_key]
		return corr_value


def main():
    nodes = Nodelist()
    node_key = raw_input('Enter the key of this node -> ')
    node_key = abs(int(node_key))
    ip_self = [(s.connect(('8.8.8.8',80)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]
    port_no = raw_input('Enter Port -> ')
    ip = ip_self+':'+port_no
    global file_name
    file_name = raw_input('Enter the file name for the data to be saved ->')
    
    contact = raw_input('Is this the first node? y/n -> ')
    if contact == 'y':
		nodes.node_join(ip, node_key)
		print (nodes.list_contents())
    else:
		nodes.contact_node()
		print("step 1")
		nodes.node_join(ip, node_key)
		print("step 2")
		nodes.update_node(ip, node_key)
		print("step 3")
		nodes.transfer_data(ip,node_key)
		print("step 4")
    global daemon_new
    daemon_new = Pyro4.Daemon(host = ip_self, port = int(port_no))
    Pyro4.Daemon.serveSimple(
            {
                nodes: 'main'
            },
            daemon=daemon_new,
            ns = False,
            verbose = True)
    
    
if __name__=="__main__":
    main()
