import threading
import requests
import logging
import time
import concurrent.futures
import random 
import queue



logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG, datefmt="%H:%M:%S")

class WebScrapper:
    def __init__(self, website_list):
        self.website_list = website_list
        self.logger = logging.getLogger(__name__)
        self.SENTINEL = object()
        self.event = threading.Event()
        pass

    def normal_scrape(self):
        total_time = 0
        for site in self.website_list:

            start = time.time()
            site = "http://" + site
            response = requests.get(site)
            duration = time.time() - start
            total_time += duration

            #self.logger.info(f"Website: {site}, Took: {duration}, Code: {response.status_code}")
        
        self.logger.info(f"\nTotal time: {total_time}")

    
    def thread_function(self, name):
        self.logger.info("Thread %s: starting", name)
        time.sleep(2)
        self.logger.info("Thread %s: finishing", name)


    def threaded_exp1(self):
        self.logger.info("Main    : before creating thread")
        
        x = threading.Thread(target=self.thread_function, args=(1,), daemon=True)
        self.logger.info("Main    : before running thread")
        
        x.start()
        self.logger.info("Main    : wait for the thread to finish")
        
        x.join()
        self.logger.info("Main    : all done")

        """
        Takeaway: Pretty simple. Daemon thread can get killed if program itself
        finishes first. x.join forces the program to wait till the thread is finished.
        """

    
    def threaded_exp2(self):
        threads = []

        for index in range(3):
            self.logger.info("Main    : create and start thread %d.", index)
            x = threading.Thread(target=self.thread_function, args=(index,))
            threads.append(x)
            x.start()

        for index, thread in enumerate(threads):
            self.logger.info("Main    : before joining thread %d.", index)
            thread.join()
            self.logger.info("Main    : thread %d done", index)

        """
        Takeaway: The order of finishing can vary and is determined by the 
        operating system. 
        Future Experiment: what would happen if I put the thread.join
        in the first loop.
        """

    def threaded_exp3(self):
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(self.thread_function, range(3))

        """
        Takeaway: This is a much easier way to set up exp2. The "with" is 
        critical because it auto executes the thread.join(). Should always
        do it this way. Process still determined by operating system so no real
        completion order.
        """

    class FakeDatabase:
        def __init__(self, logger):
            self.value = 0
            self.logger = logger
            self._lock = threading.Lock()

        def update(self, name):
            self.logger.info("Thread %s: starting update", name)
            local_copy = self.value
            local_copy += 1
            time.sleep(0.1)
            self.value = local_copy
            self.logger.info("Thread %s: finishing update", name)

        def locked_update(self, name):
            self.logger.info("Thread %s: starting update", name)
            self.logger.debug("Thread %s about to lock", name)
            with self._lock:
                self.logger.debug("Thread %s has lock", name)
                local_copy = self.value
                local_copy += 1
                time.sleep(0.1)
                self.value = local_copy
                self.logger.debug("Thread %s about to release lock", name)
            self.logger.debug("Thread %s after release", name)
            self.logger.info("Thread %s: finishing update", name)


    def threaded_exp4(self):
        database = self.FakeDatabase(self.logger)
        self.logger.info("Testing update. Starting value is %d.", database.value)

        # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        #     for index in range(2):
        #         executor.submit(database.update, index)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            for index in range(2):
                executor.submit(database.locked_update, index)
        
        self.logger.info("Testing update. Ending value is %d.", database.value)

    """
    Takeaway: Basically, the thing here is to introduce the concept of race conditions
    where different threads try to access the same resource at the same time.
    Running this example, we can see a result where the result is 1 and not 2 which is counter-
    intuitive. 

    Takeaway2: Race can be solved with the lock. It can be controlled with a "with"
    statement as well to control when things lock and unlock. Use it was often as possible
    to control race condition.

    Takeaway3: RLock is another type of lock that allows thread to acquire an RLock 
    multiple times before it calls release. 
    """

    class Pipeline:
        """
        Class to allow a single element pipeline between producer and consumer.
        """
        def __init__(self, logger):
            self.message = 0
            self.producer_lock = threading.Lock()
            self.consumer_lock = threading.Lock()
            self.consumer_lock.acquire()
            self.logger = logger

        def get_message(self, name):
            self.logger.debug("%s:about to acquire getlock", name)
            self.consumer_lock.acquire()
            self.logger.debug("%s:have getlock", name)
            message = self.message
            self.logger.debug("%s:about to release setlock", name)
            self.producer_lock.release()
            self.logger.debug("%s:setlock released", name)
            return message

        def set_message(self, message, name):
            self.logger.debug("%s:about to acquire setlock", name)
            self.producer_lock.acquire()
            self.logger.debug("%s:have setlock", name)
            self.message = message
            self.logger.debug("%s:about to release getlock", name)
            self.consumer_lock.release()
            self.logger.debug("%s:getlock released", name)


    def producer(self, pipeline):
        """Pretend we're getting a message from the network."""

        for index in range(10):
            message = random.randint(1, 101)
            self.logger.info("Producer got message: %s", message)
            pipeline.set_message(message, "Producer")

        # Send a sentinel message to tell consumer we're done
        pipeline.set_message(self.SENTINEL, "Producer")

    def consumer(self, pipeline):
        """Pretend we're saving a number in the database."""
        message = 0
        while message is not self.SENTINEL:
            message = pipeline.get_message("Consumer")
            if message is not self.SENTINEL:
                logging.info("Consumer storing message: %s", message)

    def threaded_exp5(self):
        pipeline = self.Pipeline(self.logger)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.producer, pipeline)
            executor.submit(self.consumer, pipeline)

    
    """ 
    Takeaway: This was an interesting example to illustrate the locking 
    mechanism in detail. It was a bit hard to follow but I guess the main takeaway
    for now is that just a simple SENTINEL object is not the greatest because 
    there is still some sort of access issue that can happen between threads.
    """


    class Pipeline_queue(queue.Queue):
        def __init__(self, logger):
            super().__init__(maxsize=10)
            self.logger = logger

        def get_message(self, name):
            self.logger.debug("%s:about to get from queue", name)
            value = self.get()
            self.logger.debug("%s:got %d from queue", name, value)
            
            return value

        def set_message(self, value, name):
            self.logger.debug("%s:about to add %d to queue", name, value)
            self.put(value)
            self.logger.debug("%s:added %d to queue", name, value)



    def producer_queue(self, pipeline, event):
        """Pretend we're getting a number from the network."""
        while not event.is_set():
            message = random.randint(1, 101)
            self.logger.info("Producer got message: %s", message)
            pipeline.set_message(message, "Producer")

        logging.info("Producer received EXIT event. Exiting")

    def consumer_queue(self, pipeline, event):
        """Pretend we're saving a number in the database."""
        while not event.is_set() or not pipeline.empty():
            message = pipeline.get_message("Consumer")
            self.logger.info(
                "Consumer storing message: %s  (queue size=%s)",
                message,
                pipeline.qsize(),
            )

        logging.info("Consumer received EXIT event. Exiting")

    def threaded_exp6(self):
        pipeline = self.Pipeline_queue(self.logger)
        event = self.event
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.producer_queue, pipeline, event)
            executor.submit(self.consumer_queue, pipeline, event)

            time.sleep(0.1)
            self.logger.info("Main: about to set event")
            event.set()

    """
    Takeway: This is a better example with queue but its still not the best because
    we don't need the pipeline object since it essentially serves on top of a queue
    which has functions itself to do things.  
    """

    def producer_queue_good(self, queue, event):
        """Pretend we're getting a number from the network."""
        while not event.is_set():
            message = random.randint(1, 101)
            self.logger.info("Producer got message: %s", message)
            queue.put(message)

        logging.info("Producer received event. Exiting")


    def consumer_queue_good(self, queue, event):
        """Pretend we're saving a number in the database."""
        while not event.is_set() or not queue.empty():
            message = queue.get()
            self.logger.info(
                "Consumer storing message: %s (size=%d)", message, queue.qsize()
            )

        logging.info("Consumer received event. Exiting")

    def threaded_exp7(self):
        pipeline = queue.Queue(maxsize=10)
        event = self.event
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.producer_queue_good, pipeline, event)
            executor.submit(self.consumer_queue_good, pipeline, event)

            time.sleep(0.1)
            logging.info("Main: about to set event")
            event.set()




website_list = ["www.google.com", "www.yahoo.com", "www.ign.com", "www.theatlantic.com", "www.bestbuy.com",
                "www.amazon.com", "www.bbc.co.uk", "www.reddit.com", "www.tsn.ca", "www.youtube.com"]


#WebScrapper(website_list).normal_scrape()

WebScrapper(website_list).threaded_exp7()
