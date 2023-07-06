import typing
import time
from multiprocessing import Process, Queue
import csv
import random

number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1

class add_exclamations:
    
    def __init__(self,d):
        self.d = d
    
    def execute(self):
        print(self.d + '!!')
        return self.d + '!!'

class add_question_marks:
    
    def __init__(self,d):
        self.d = d
    
    def execute(self):
        print(self.d + '??')
        return self.d + '??'


def mpi_application(
        rank:int,
        size:int,
        send_f:typing.Callable[[typing.Any,int],None],
        recv_f:typing.Callable[[int], typing.Any]
    ):

    if rank == 0: #COORDINATOR LOGIC
        task_types = [add_exclamations, add_question_marks]
        for rank_to_send_to in range(1,size):
            cls_task_type = random.choice(task_types)
            send_f(cls_task_type('hello world'),dest=rank_to_send_to)
        with open('output_csv.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            for _ in range(1, size):
                value = recv_f(MPI_ANY_SOURCE)
                writer.writerow([value])
    else: #WORKER LOGIC
        task = recv_f(MPI_ANY_SOURCE)
        result = task.execute()
        send_f(result, dest = 0)

    # NOTE for the assignment you can not specify the reception of a message
    # from a single source, you only need to receive from any source using:

    # data = recv_f(MPI_ANY_SOURCE)


    # NOTE to send a message/data from a process to process with rank 2, you
    # use:

    # send_f(data,2)


    # NOTE ensure the coordinator sends a message to inform each process to
    # end and the coordinator should end as well.  If the application
    # does not end, you likely messed this up.

    # TODO implement your MPI application logic here using the parameters above
    # instead of mpi4py


###############################################################################
# This is the simulator code, do not adjust

def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)
    
    app_f(process_rank, size, send_f, recv_f)

def _generate_recv_f(process_rank, send_queues):

    def recv_f(from_source:int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f


def _generate_send_f(process_rank, send_queues):

    def send_F(data, dest):
        send_queues[dest].put((process_rank,data))
    return send_F


def _simulate_mpi(n:int, app_f):
    
    send_queues = {}

    for process_rank in range(n):
        send_queues[process_rank] = Queue()
    
    ps = []
    for process_rank in range(n):
        
        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
###############################################################################


if __name__ == "__main__":
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
