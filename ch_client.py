import bisect
import md5
import requests
import json


class ConsistentHashRing():
    """Implement a consistent hashing ring."""

    def __init__(self):
        """Create a new ConsistentHashRing.

        :param replicas: number of replicas.

        """
        self._keys = []
        self._nodes = {}

    def _hash(self, key):
        """Given a string key, return a hash value."""
        #print "key="+key
        #print "hash="+str(md5.md5(key).hexdigest())
        return long(md5.md5(key).hexdigest(), 16)
	
    def __setitem__(self, nodename, node):
        """Add a node, given its name.

        The given nodename is hashed
        among the number of replicas.

        """
        hash_ = self._hash(str(nodename))
        if str(hash_) in self._nodes:
            raise ValueError("Node name %r is already present" % nodename)
        else:
            self._nodes[hash_] = node
            bisect.insort(self._keys, hash_)
            #print "ring="+str(self._keys)

    def __delitem__(self, nodename):
        """Remove a node, given its name."""

        hash_=self._hash(str(nodename))
        if str(hash_) in self._nodes:
            del self._nodes[hash_]
            index = bisect.bisect_left(self._keys, hash_)
            del self._keys[index]
        else:
            raise ValueError("Node name %r is "
                            "not present" % nodename)
        

    def __getitem__(self, key):
        """Return a node, given a key.

        The node replica with a hash value nearest
        but not less than that of the given
        name is returned.   If the hash of the
        given name is greater than the greatest
        hash, returns the lowest hashed node.

        """
        hash_ = self._hash(key)
        start = bisect.bisect(self._keys, hash_)
        if start == len(self._keys):
            start = 0
        return self._nodes[self._keys[start]]


def main():
	cr = ConsistentHashRing()
    cr.__setitem__("server1","http://127.0.0.1:3000")
	cr.__setitem__("server2","http://127.0.0.1:5050")
	cr.__setitem__("server3","http://127.0.0.1:8080")
	request_object1={"id" : "1",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object2={"id" : "2",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object3={"id" : "3",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object4={"id" : "4",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object5={"id" : "5",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object6={"id" : "6",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object7={"id" : "7",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object8={"id" : "8",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object9={"id" : "9",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
    "submit_date" : "12-10-2016"}
	request_object10={"id" : "10",
    "name" : "Foo 1",
    "email" : "foo1@bar.com",
    "category" : "office supplies",
    "description" : "iPad for office use",
    "link" : "http://www.apple.com/shop/buy-ipad/ipad-pro",
    "estimated_costs" : "700",
	"submit_date" : "12-10-2016"}
	request_object={"1":{},"2":{},"3":{},"4":{},"5":{},"6":{},"7":{},"8":{},"9":{},"10":{}}
	request_object["1"]=request_object1
	request_object["2"]=request_object2
	request_object["3"]=request_object3
	request_object["4"]=request_object4
	request_object["5"]=request_object5
	request_object["6"]=request_object6
	request_object["7"]=request_object7
	request_object["8"]=request_object8
	request_object["9"]=request_object9
	request_object["10"]=request_object10
	d=[]
	d.append("0")
	for i in range(1,11):
		input_key=request_object[str(i)]["id"]
		d.append(cr.__getitem__(input_key))
	
	for i in range(1,11):
		url=str(d[i])+"/v1/expenses/"+str(i)
		#print url
		requests.post(url,data=json.dumps(request_object[str(i)]))
		

	d_get=[]
	d_get.append(0)
	response_object={"1":{},"2":{},"3":{},"4":{},"5":{},"6":{},"7":{},"8":{},"9":{},"10":{}}
	
	for i in range(1,11):
		input_key=request_object[str(i)]["id"]
		d_get.append(cr.__getitem__(input_key))
    
	for i in range(1,11):
		url=str(d[i])+"/v1/expenses/"+str(i)
		response_object[str(i)]=requests.get(url).json()
	print "Get result"
	print response_object

if  __name__ =='__main__':
    main()
