#!/usr/bin/python

#########################################
#										#
#	All the local operations are here   #
#										#
#########################################

import storage
import node
import readline
import re
import ctypes
import Pyro4


## WAITING FOR COMMANDS

def replace_line(file_name, line_num, text):
	lines = open(file_name, 'r').readlines()
	lines[line_num] = text
	out = open(file_name, 'w')
	out.writelines(lines)
	out.close()

def user_input():
	while 1:
		
		info="\nCommand to update: update(key,value,node_address) [Node Address format (IP:Port)]\nCommnad to get: get(key,node_address)\nCommand to leave: leave(IP:Port)"
		print info
		input_update=raw_input('->')
		command=re.match(r"(\w+)",input_update)
		
		# update operation

		if command.group() == 'update':
			ip=re.search(r"(\d+),(.*),(.*)\)",input_update)
			u_key=abs(int(ip.group(1)))
			u_value=ip.group(2)
			node_addr=ip.group(3)
			print ("\nYour input: key-> %d, value-> %s, node address-> %s\n" %(u_key,u_value,node_addr))
			KVpair = {}
			KVpair[u_key] = u_value
			uri = "PYRO:main@" + node_addr
			remote = Pyro4.Proxy(uri)
			remote.store_nodes(KVpair,node_addr)
			KVpair.clear()
			
		elif command.group() == 'get':
			ip=re.search(r"(\d+),(.*)\)",input_update)
			key = abs(int(ip.group(1)))
			node_addr = ip.group(2)
			print ("\nYour input: key-> %d\n" %(key))
			uri = "PYRO:main@" + node_addr
			remote = Pyro4.Proxy(uri)
			req_value = remote.get_val(key)
			print("\nkey-> %d value-> %s\n" %(key,req_value))
		
		# leave operation	
			
		elif command.group() == 'leave':
			ip=re.search(r"\((.*)\)",input_update)
			node_addr = ip.group(1)
			uri = "PYRO:main@" + node_addr
			remote = Pyro4.Proxy(uri)
			remote.node_leave(uri,node_addr)
			
		else:
			print ("\nCommand not found\n")

if __name__=="__main__":
    user_input()


"""
			# Updating duplicates
			
			checkpoint=0
			with open('KeyValueData.txt','r') as file:
				for i, line in enumerate(file, 1):
					string='%s \t '%(str(u_key))
					if string in line:
						line_num = i-1
						print line_num
						replace_line('KeyValueData.txt', line_num, "%u \t %s\n" %(u_key,u_value))
						checkpoint=1
				
			# Updating new
			
			if checkpoint==0:
				OutputFile=open("KeyValueData.txt", 'a')
				OutputFile.write("%u \t %s\n" %(u_key,u_value))
				OutputFile.close()
			"""			
