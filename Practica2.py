#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 15:06:21 2022

@author: daniel
"""
"""Tenemos NPROD procesos que producen números no negativos de forma
creciente. Cuando un proceso acaba de producir, produce un -1.
El consumidor toma el menor numero y lo almacena en una lista llamada merge de
forma creciente. """

from multiprocessing import Process, Manager
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
import random


N = 200
K = 1
NPROD = 3
NCONS = 1


def add_data(storage, pid, mutex):
    mutex.acquire()
    try:
        storage[pid] = storage[pid] + random.randint(0,5)
    finally:
        mutex.release()
  
#funcion que devuelve el minimo del almacen y la posicion en la que se encuentra
#ya que necesitamos saber que parte del almacen se ha quedado vacio para saber
#que proceso debe producir
def get_minimo(storage, mutex, running):
    mutex.acquire()
    try:
        valores = []
        rango = [i for i in range(0, len(running)) if running[i]==True]
        for i in rango:
            if storage[i] != -1:
                valores.append(storage[i])
            else:
                running[i] = False
        minimo = min(valores)
        almacen = list(storage)
    finally:
        mutex.release()
    return minimo,almacen.index(minimo)


def producer(storage, empty, non_empty, mutex, merge):
    pid = int(current_process().name.split('_')[1])#indice asociado al productor
    for i in range(N):
        print (f"prod {pid} produciendo")
        empty.acquire()
        add_data(storage, pid, mutex)#añadimos el valor que produzcamos al almacen
        non_empty.release()
        print(list(storage))
        print(f"prod {pid} almacenando")  
    storage[pid]= -1


def consumer(storage, empty, non_empty, mutex, merge):
    for i in range(NPROD):
        non_empty[i].acquire()
    running = [True]*NPROD
    while True in running:
        m,j = get_minimo(storage, mutex,running) #obtenemos el minimo del almacen
        merge.append(m) #almacenamos el minimo en merge
        empty[j].release() #damos permiso al proceso j para producir
        print (f"consumiendo {m}")
        non_empty[j].acquire()
        if list(storage) == [-1]*NPROD:
            running = [False]*NPROD
    print("Ya no se puede consumir")
    print("\nmerge:",list(merge))
        

def main():
    storage = Array('i', NPROD*K) #almacen donde porduciremos los enteros
    print ("almacen inicial", storage[:])
    
    #creamos una lista de semáforos, empty[i] sera el semaforo del proceso i
    empty = [0]*NPROD
    non_empty = [0]*NPROD
    for i in range(NPROD):
        empty[i] = BoundedSemaphore(K)
        non_empty[i] = Semaphore(0)
    
    mutex = Lock()
    
    manager = Manager()
    merge = manager.list() #lista  del resultado, enteros ordenados de forma creciente
    
    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(storage, empty[i], non_empty[i], mutex, merge))
                for i in range(NPROD) ]

    conslst = [Process(target=consumer,
                      name="cons_{i}",
                      args=(storage, empty, non_empty, mutex, merge))]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()
    
   
if __name__ == '__main__':
    main()