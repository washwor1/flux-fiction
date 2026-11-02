import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)
import flux

def sim_exec_start_cb(flux_handle, watcher, msg, args=None):
    logger.debug("sim_exec_start_cb called")
    try:
        ctx = watcher.exec_ctx
        payload = msg.payload
        jobid = payload["id"]

        if payload.get("reattach"):
            # A restarted job-manager hands back a job that was already
            # running when the instance was checkpointed. It must not count as
            # a new start: keep the request for the eventual finish/release and
            # answer "reattached" (the job-manager rejects a "start" here).
            ctx["pending_complete_msgs"][jobid] = msg
            flux_handle.respond(msg, payload={"id": jobid, "type": "reattached", "data": {}})
            on_reattach = ctx.get("on_reattach")
            if on_reattach is not None:
                on_reattach(jobid)
            return

        ctx["pending_start_msgs"][jobid] = msg

        if ctx.get("batch_job_starts", True):
            flux_handle.rpc(
                "job-manager.emu-jobtap.buffer-start",
                payload={"jobid": jobid}
            ).then(lambda fut, arg: None, arg=None)
        else:
            ctx["start_job"](jobid)

    except Exception as e:
        logger.debug(f"Error starting the job: {e}")


def sim_exec_flush_starts_cb(flux_handle, watcher, msg, arg=None):

    logger.debug("sim_exec_flush_starts_cb called")

    try: 
        ctx = watcher.exec_ctx
        body = msg.payload or {}
        jobids = body.get("jobids", [])

        for jobid in jobids:
            # Move start msg to the complete msg list to be used in ack_complete in FluxAdapter
            ctx["start_job"](jobid)
             

        flux_handle.respond(msg, payload={"ok": True})
    except Exception as e:
        logger.exception("sim_exec_flush_starts_cb failed: %r", e)



def exec_hello(flux_handle):
    '''
    Registers the simple exec module as the exec system in the job manager
    '''
    logger.debug("Registering sim-exec with job-manager")
    flux_handle.rpc("job-manager.exec-hello",
                    payload={"service": "sim-exec"}).get()


def service_add(f, name):
    future = f.service_register(name)
    return f.future_get(future, None)


def service_remove(f, name):
    future = f.service_unregister(name)
    return f.future_get(future, None)

def setup_watchers(flux_handle, start_job_cb: Callable, pending_start_msgs: dict, pending_complete_msgs: dict, batch_job_starts: bool = True, on_reattach: Callable | None = None):
    '''
    Adds all appropriate watchers to the emulator

    Currently, only adds one to watch for "sim-exec.start"
    '''
    watchers = []
    services = set()

    exec_ctx = {
        "pending_start_msgs": pending_start_msgs,
        "pending_complete_msgs": pending_complete_msgs,
        "start_job": start_job_cb,
        "batch_job_starts": bool(batch_job_starts),
        "on_reattach": on_reattach,
    }

    for type_mask, topic, cb in [
        (flux.constants.FLUX_MSGTYPE_REQUEST, "sim-exec.start", sim_exec_start_cb),
        (flux.constants.FLUX_MSGTYPE_REQUEST, "sim-exec.flush-starts",  sim_exec_flush_starts_cb),
    ]:
        watcher = flux_handle.msg_watcher_create(
            cb, type_mask=type_mask, topic_glob=topic
        )
        watcher.exec_ctx = exec_ctx
        watcher.start()
        watchers.append(watcher)
        if type_mask == flux.constants.FLUX_MSGTYPE_REQUEST:
            service_name = topic.split(".")[0]
            if service_name not in services:
                service_add(flux_handle, service_name)
                services.add(service_name)
    return watchers, services


def teardown_watchers(flux_handle, watchers, services):
    '''
    Destructs watchers
    '''
    for watcher in watchers:
        watcher.stop()
    for service_name in services:
        service_remove(flux_handle, service_name)