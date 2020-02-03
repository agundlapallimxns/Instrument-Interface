from socket import *
network = '172.16.40.'
def is_up(addr):
    s = socket(AF_INET, SOCK_STREAM)
    s.settimeout(0.01)    ## set a timeout of 0.01 sec
    if not s.connect_ex((addr,135)):    # connect to the remote host on port 135
        s.close()                       ## (port 135 is always open on Windows machines, AFAIK)
        return 1
    else:
        s.close()

def run():
    for ip in range(1,256):    ## 'ping' addresses 192.168.1.1 to .1.255
        addr = network + str(ip)
        if is_up(addr):
            print('%s \t- %s' %(addr, getfqdn(addr)))## the function 'getfqdn' returns the remote hostname
    print('-------')    ## just print a blank line

if __name__ == '__main__':
    print('''I'm scanning the local network for connected Windows machines (and others with samba server running).
This might take some time, depending on the number of the PC's found. Please wait...''')

run()