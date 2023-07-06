import typing
import time
from multiprocessing import Process, Queue


number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1

def mpi_application(
        rank:int,
        size:int,
        send_f:typing.Callable[[typing.Any,int],None],
        recv_f:typing.Callable[[int], typing.Any]
    ):
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
    if rank == 0:
        # TODO implement coordinator logic
        send_f(data,rank)
        print(f"Process with rank {rank} sending the following data: {data}")
    else:
        # TODO implement worker logic
        data = recv_f(MPI_ANY_SOURCE)
        print(f"Process with rank {rank} received the following data: {data}")


###############################################################################
# This is the simulator code, do not adjust


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
            target=app_f,
            args=(
                process_rank,
                n,
                _generate_send_f(process_rank, send_queues),
                _generate_recv_f(process_rank, send_queues)
            )
        )
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
###############################################################################


if __name__ == "__main__":
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
