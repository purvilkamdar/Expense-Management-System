from functools import wraps
from datetime import datetime, timedelta
import requests, time
import redis
from time import sleep

r = redis.Redis(host='localhost', port=6379, db=0)

class CircuitBreaker(object):
    def __init__(self, name=None, expected_exception=Exception, max_failure_to_open=3, reset_timeout=10):
        self._name = name
        self._expected_exception = expected_exception
        self._max_failure_to_open = max_failure_to_open
        self._reset_timeout = reset_timeout
        self.close()
 
    def close(self):
        self._is_closed = True
        self._failure_count = 0
        
    def open(self):
        self._is_closed = False
        self._opened_since = datetime.utcnow()
        
    def can_execute(self):
        if not self._is_closed:
            self._open_until = self._opened_since + timedelta(seconds=self._reset_timeout)
            self._open_remaining = (self._open_until - datetime.utcnow()).total_seconds()
            return self._open_remaining <= 0
        else:
            return True

    def __call__(self, func):
        if self._name is None:
            self._name = func.__name__

        @wraps(func)
        def with_circuitbreaker(*args, **kwargs):
            return self.call(func, *args, **kwargs)

        return with_circuitbreaker
    #all changes done here-neha
    def call(self, func, *args, **kwargs):
        bad_server = r.lindex("hosts",0)
	print "cb called"
        if not self.can_execute():	  
	    print "cb is in open state"
            print "removing %s server"%r.lindex("hosts",0)  
	    r.lrem("hosts",bad_server, 0)
            nexser = r.lindex("hosts",0)
	    print "Connecting to next server: %s" %nexser
	 
            err = 'CircuitBreaker[%s] is OPEN until %s (%d failures, %d sec remaining)' % (
                self._name,
                self._open_until,
                self._failure_count,
                round(self._open_remaining)
            )
            raise Exception(err)
	          
        try:     
            bd_serv= "http://"+bad_server+"/v1/expenses"
            print "retrying to connecting on %s" %bd_serv
	    result = requests.get(bd_serv)  	 
          
        except Exception, e:
	    self._failure_count += 1
            if self._failure_count >= self._max_failure_to_open:
                self.open()
            raise
        self.close()
        return result

