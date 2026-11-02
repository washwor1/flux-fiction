from flux_fiction._core.models import Makespan
import logging
logger = logging.getLogger(__name__)

class SimpleExec(object):
    '''
    This module is a simulator for job execution. It is loaded like a broker module in Flux

    The exact behavior of SimpleExec currently is to recieve start job notifications from the 
    job manager in Flux, notify the user-event simulator, mark down bookkeeping information
    about jobs, send an ackowledgment to the job manager that jobs are starting, and handle
    requests from the user-event simulator to end jobs.

    One behavior to note is that this will currently buffer the start acknowledgements for jobs
    to the job manager and flush them at the end of each timestep in the user-event simulator.
    This is planned to become togglable soon. It is more realistic to not batch the acks but 
    it is useful to make times line up properly.
    '''
    def __init__(self, num_nodes, cores_per_node, gpus_per_node=0, exclusive=False):
        self.num_nodes = num_nodes
        self.cores_per_node = cores_per_node
        self.gpus_per_node = int(gpus_per_node or 0)
        self.exclusive = bool(exclusive)
        self.num_free_nodes = num_nodes
        self.used_core_hours = 0
        self.used_gpu_hours = 0

        self.makespan = Makespan(
            beginning=float('inf'),
            end=-1,
        )

    def to_state(self):
        return {
            "num_free_nodes": self.num_free_nodes,
            "used_core_hours": self.used_core_hours,
            "used_gpu_hours": self.used_gpu_hours,
            "makespan_beginning": self.makespan.beginning,
            "makespan_end": self.makespan.end,
        }

    def load_state(self, state):
        self.num_free_nodes = state["num_free_nodes"]
        self.used_core_hours = state["used_core_hours"]
        self.used_gpu_hours = state["used_gpu_hours"]
        self.makespan = Makespan(
            beginning=float(state["makespan_beginning"]),
            end=float(state["makespan_end"]),
        )

    def update_makespan(self, current_time):
        '''
        Helper function that allows you to modify the makespan
        '''
        if current_time < self.makespan.beginning:
            self.makespan = self.makespan._replace(beginning=current_time)
        if current_time > self.makespan.end:
            self.makespan = self.makespan._replace(end=current_time)

    def submit_job(self, simulation, job):
        '''
        Updates the makespan on job submission
        '''
        self.update_makespan(simulation.current_time)

    def start_job(self, simulation, job):
        '''
        Checks to make sure the job requirements are feasible for jobs that are starting

        #TODO This does not work properly when allocating less cores than an entire node
        '''
        self.num_free_nodes -= job.nnodes
        if self.num_free_nodes < 0:
            logger.error("Scheduler over-subscribed nodes")

        if not self.exclusive:
            if (job.ncpus / job.nnodes) > self.cores_per_node:
                logger.error("Scheduler over-subscribed cores on the node")
            if job.ngpus:
                if not self.gpus_per_node:
                    logger.error("Job requested GPUs but system has none configured")
                elif (job.ngpus / job.nnodes) > self.gpus_per_node:
                    logger.error("Scheduler over-subscribed GPUs on the node")

    def complete_job(self, simulation, job):
        '''
        Updates the makespan for jobs that complete
        '''
        self.num_free_nodes += job.nnodes
        if self.exclusive:
            self.used_core_hours += (self.cores_per_node * job.nnodes * job.elapsed_time) / 3600
            if self.gpus_per_node:
                self.used_gpu_hours += (self.gpus_per_node * job.nnodes * job.elapsed_time) / 3600
        else:
            self.used_core_hours += (job.ncpus * job.elapsed_time) / 3600
            if job.ngpus:
                self.used_gpu_hours += (job.ngpus * job.elapsed_time) / 3600
        self.update_makespan(simulation.current_time)

    def post_analysis(self, simulation):
        """
        Outputs statistics about the simulation whenever called.
        """
        if self.makespan.beginning > self.makespan.end:
            logger.warning(
                "Makespan beginning ({}) greater than end ({})".format(
                    self.makespan.beginning, self.makespan.end
                )
            )

        makespan_hours = max(0.0, (self.makespan.end - self.makespan.beginning) / 3600.0)

        # Core stats
        total_core_hours = self.num_nodes * self.cores_per_node * makespan_hours
        print("Makespan (hours): {:.1f}".format(makespan_hours))
        print("Total Core-Hours: {:,.1f}".format(total_core_hours))
        print("Used Core-Hours: {:,.1f}".format(self.used_core_hours))
        if total_core_hours > 0:
            print("Average Core-Utilization: {:.2f}%".format(
                (self.used_core_hours / total_core_hours) * 100.0
            ))
        else:
            logger.error("Total core hours is 0. Simulation likely didn't run or no jobs were submitted.")

        # GPU stats (only if GPUs are configured)
        if self.gpus_per_node:
            total_gpu_hours = self.num_nodes * self.gpus_per_node * makespan_hours
            print("Total GPU-Hours: {:,.1f}".format(total_gpu_hours))
            print("Used GPU-Hours:  {:,.1f}".format(self.used_gpu_hours))
            if total_gpu_hours > 0:
                print("Average GPU-Utilization: {:.2f}%".format(
                    (self.used_gpu_hours / total_gpu_hours) * 100.0
                ))
            else:
                logger.error("Total GPU hours is 0. (GPUs configured but zero makespan?)")