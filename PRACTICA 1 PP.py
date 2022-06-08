#!/usr/bin/env python3
# -- coding: utf-8 --

"""
@author: jaime
"""
from typing import *
from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random

SEQUENCE_END_SIGNAL = -1

# Total ammount of items to be produced by each process
ITEMS_PER_PROCESS = 100
K = 10
NPROD = 3
NCONS = 3

TOTAL_ITEMS_ACROSS_PROCESSES = ITEMS_PER_PROCESS * NPROD


def random_factor_delay(factor: int = 3) -> None:
    """
    Waits a random amount of time between 0 and factor seconds.
    """
    sleep(random()/factor)


def add_data_to_process_storage(storage: Array, index: Value, data: int, mutex: Lock):
    """
    Adds data to the process with "index" in the storage.
    """
    print(f"ADDING {data} to PROCESS[{index}]")
    mutex.acquire() # Lock the index
    try:
        storage[index.value] = data
        random_factor_delay(6)
        index.value += index.value + 1
    finally:
        mutex.release()


def retrieve_data_from_process_storage(storage: Array, index: Value, mutex: Lock) -> int:
    """
    Retrieves the data for process with "index" from the storage.
    The Lock is used to protect the index.
    """
    mutex.acquire() # Lock the index

    try:
        data = storage[0]
        index.value = index.value - 1
        random_factor_delay(6)

        # Set the current value equal to the next
        for i in range(index.value):
            storage[i] = storage[i + 1]
        
        # Set the end signal 
        storage[index.value] = -1

    finally:
        mutex.release() 
    return data


def find_lowest_value_index(storage: Sequence[Array]) -> int:
    """
    Finds the index of the lowest value in the storage.
    """

    lowest_index = None # Lowest value among processes
    index = -1 

    # Iterate over all processes
    for i in range(NPROD):
        if storage[i][0] == -1: 
            continue

        # If not set yet, set the lowest index
        if index == -1:
            index = i
            lowest_index = storage[i][0]
        
        # If the current value is lower than the lowest value, set it as the lowest value
        if storage[i][0] < lowest_index:  
            lowest_index = storage[i][0]
            index = i
    return index


def producer (
    local_storage: Array,
    local_index: Value, 
    local_empty_semaphore: BoundedSemaphore,
    local_non_empty_semaphore: Semaphore, 
    local_mutex: Lock
) -> None:
    """
    Producer process function. Stores a sequence of numbers in the local storage.
    """
    for item in range(ITEMS_PER_PROCESS): 
        random_factor_delay(6)

        # Add the current sequential item as data to the process storage
        local_empty_semaphore.acquire()        
        add_data_to_process_storage(
            local_storage, local_index, item, local_mutex
        )
        local_non_empty_semaphore.release()

        print(f"PPRODUCER[{current_process().name}] <--storing-- {item}")

    # Store the end signal
    local_empty_semaphore.acquire()
    add_data_to_process_storage(
        local_storage, local_index, SEQUENCE_END_SIGNAL, local_mutex
    )
    local_non_empty_semaphore.release()

    print(f"PPRODUCER[{current_process().name}] DONE")


def consumer (
    global_storage: Sequence[Array], 
    global_indexes: Sequence[Value],
    global_empty_semaphores: Sequence[BoundedSemaphore],
    global_non_empty_semaphores: Sequence[Semaphore], 
    global_mutexes: Sequence[Lock], 
    global_sorted_array: Sequence[int]
) -> None:
    """
    Consumer process function.
    """
    semaphore_acquisitions = [False for i in range(NPROD)]
    random_factor_delay(6)

    # For each value in each process...
    for global_item_index in range(TOTAL_ITEMS_ACROSS_PROCESSES):
        for process_index in range(NPROD):

            # If the process has not acquired the semaphore yet...
            if not semaphore_acquisitions[process_index]:

                # Acquire the semaphore
                global_non_empty_semaphores[process_index].acquire()

                # Mark the process as having acquired the semaphore
                semaphore_acquisitions[process_index] = True

        # Find the index of the lowest value in the storage
        lowest_value_index = find_lowest_value_index(global_storage)

        # Set its semaphore to False, so it can be acquired again
        semaphore_acquisitions[lowest_value_index] = False
        consumed_value = retrieve_data_from_process_storage(
            global_storage[lowest_value_index],
            global_indexes[lowest_value_index], 
            global_mutexes[lowest_value_index]
        )

        # Release the semaphore for the process with the lowest value
        global_empty_semaphores[lowest_value_index].release()

        global_sorted_array[global_item_index] = consumed_value
        random_factor_delay(6)
        
        print(f"PCONSUMER[{current_process().name}] <--consuming-- {consumed_value} from PRODUCER[{lowest_value_index}]")
        
def main():

    # Process shared data
    global_process_values_storage: Sequence[Sequence[int]] = []

    # Process shared indexes
    global_process_indexes: Sequence[int] = []

    # Process shared mutexes
    global_process_mutexes: Sequence[Lock] = []

    # All processes
    global_process_array: Sequence[Process] = []

    # Process shared non-empty semaphores
    non_empty_semaphores: Sequence[Semaphore] = []

    # Process shared empty semaphores
    empty_bounded_semaphores: Sequence[BoundedSemaphore] = []

    sorted_array = Array('i', TOTAL_ITEMS_ACROSS_PROCESSES)

    # For each process...
    for process_index in range(NPROD):

        # ...append a list of K-values (-1) to the global list
        global_process_values_storage.append(Array('i', [-1] * K))
        
        # ...append the current index (0) to the global indexes list
        global_process_indexes.append(Value('i', 0))

        # ...append a semaphore to the non-empty list
        non_empty_semaphores.append(Semaphore(0))

        # ...append a K-Limited BoundedSemaphore to the empty list
        empty_bounded_semaphores.append(BoundedSemaphore(K))

        # ...append a mutex to the global mutexes list
        global_process_mutexes.append(Lock())

    # Again, for each process...
    for process_index in range(NPROD):
        print(f"Spawning process {process_index}...")

        # ...create a new process and pass the shared data to it
        global_process_array.append(
            Process(
                target=producer,
                name=f'producer_process_{process_index}',
                args=(
                    global_process_values_storage[process_index], # Current process storage
                    global_process_indexes[process_index],        # Current process index
                    empty_bounded_semaphores[process_index],      # Current process empty semaphores
                    non_empty_semaphores[process_index],          # Current process non-empty semaphores
                    global_process_mutexes[process_index]         # Current process mutex
                )
            )
        )

    # Spawn the global merge process
    merge = Process(
        target=consumer,
        name=f'consumer_{process_index}',
        args=(
            global_process_values_storage, 
            global_process_indexes, 
            empty_bounded_semaphores, 
            non_empty_semaphores, 
            global_process_mutexes, 
            sorted_array
        )
    )

    # Start all processes
    for p in global_process_array:
        p.start()
    
    # Start the merge process
    merge.start()

    # Wait for all processes to finish
    for p in global_process_array:
        p.join()
    
    # Wait for the merge process to finish
    merge.join()

    # Print the sorted array
    print(sorted_array[:])


if _name_ == '_main_':
    main()