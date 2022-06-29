import random
from multiprocessing import (
    Process, 
    BoundedSemaphore, 
    Semaphore, 
    Lock, 
    Array
)

NPROD = 5
NCONS = 1
OFFSET = 2
N = 3

def min_product(array: list[int]) -> tuple[int, int]:
    """
    Returns the index of the minimum value in the array and its value
    """
    temp_buffer = []

    try:
        for index in range(NPROD):
            temp_buffer.append(array[OFFSET*index])
        current_buffer_index = 0
    
        minimun_value = temp_buffer[current_buffer_index]
        while temp_buffer[current_buffer_index] == -1:
            current_buffer_index += 1
        
        minimun_value = temp_buffer[current_buffer_index]
        prod = current_buffer_index

        # Find the minimum value in the buffer
        for iter_buffer_index in range(prod, len(temp_buffer)):
            if temp_buffer[iter_buffer_index] < minimun_value and temp_buffer[iter_buffer_index] != -1:
                minimun_value = temp_buffer[iter_buffer_index]
                prod = iter_buffer_index
        return minimun_value, prod

    except IndexError:
        exit(1)

def add_data(mutex, buffer, index, products, new_value):
    """
    Adds a new value to the global shared buffer.
    """
    mutex.acquire() # Lock the buffer's mutex
    try:
        buffer[(OFFSET * index) + products[index]] = new_value # Add the new value to the buffer
        products[index] += 1                                   # Increment the current product's counter
        print(" -> BUFFER:", list(buffer)) 
    finally:
        mutex.release()


def get_data(mutex, buffer, products, sorted_array):
    """
    Returns the index of the minimum value in the array and its value
    """
    mutex.acquire()
    try:
        num, prod = min_product(buffer)
        print('CONSUMER READS', num, 'from PRODUCER', prod)
        sorted_array.append(num)
        for i in range(products[prod] - 1):
            buffer[prod*OFFSET + i] = buffer[prod*OFFSET + (i+1)]
        products[prod] -= 1
    finally:
        mutex.release()
    return num, prod


def producer_fn(global_mutex_array, mutex, buffer, prod, products):
    """
    Producer function. 
    """
    produced_value = 0
    for index in range(N):
        produced_value += random.randint(0, 9) 
        global_mutex_array[2 * prod].acquire() 
        print(f'PRODUCER {prod} PRODUCES {produced_value} and has produced a total of {index + 1} values already')
        add_data(mutex, buffer, prod, products, produced_value)
        global_mutex_array[(2 * prod) + 1].release()
        
    produced_value = -1 # Indicates that the producer has finished
    global_mutex_array[2 * prod].acquire()

    # Add the last value to the buffer
    add_data(mutex, buffer, prod, products, produced_value)
    global_mutex_array[(2 * prod) + 1].release()


def consumer_fn(global_mutex_array, mutex, buffer, products):
    """
    Consumer function.
    """
    sorted_array = []
    for i in range(NPROD): 
        global_mutex_array[(2*i)+1].acquire()

    # While the producer has not finished
    while [buffer[i] for i in range(0, len(buffer))] != [-1]*(NPROD*OFFSET):
        num, prod = get_data(mutex, buffer, products, sorted_array)
        global_mutex_array[2 * prod].release()      # Release the current producer's mutex
        global_mutex_array[2 * prod + 1].acquire()  # Acquire the next producer's mutex
        print(f" -> ARRAY: {sorted_array}")
    print('CONSUMER RETURNS', sorted_array)


def main():
    buffer = Array('i', NPROD * OFFSET)
    products = Array('i', NPROD)

    semaphores = []
    for _ in range(NPROD):
        semaphores.append(BoundedSemaphore(OFFSET))
        semaphores.append(Semaphore(0))
    
    # Global shared mutex
    mutex = Lock()

    # Spawned processes
    global_process_array = []

    # Create the producer processes
    for prod in range(NPROD):
        global_process_array.append(
            Process(
                target=producer_fn, 
                args=(semaphores, mutex, buffer, prod, products)
            )
        )
    
    # Append the consumer process
    global_process_array.append(
        Process(
            target=consumer_fn, 
            args=(semaphores, mutex, buffer, products)
        )   
    )

    # Start all processes
    for p in global_process_array:
        p.start()

    # Wait for all processes to finish
    for p in global_process_array:
        p.join()

if _name_ == "_main_":
    main()