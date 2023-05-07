#!/usr/sbin/dtrace -s

#pragma D option quiet


dtrace:::BEGIN
{
    printf("Tracing lock wait times for noahtest3... Press Ctrl+C to stop.\n");
}

/* Probe when a thread starts waiting for a mutex lock */
mach_kernel:::mutex_lock_block
/execname == "noahtest3"/
{
    lock_wait_start[arg0] = timestamp;
}

/* Probe when a thread stops waiting for a mutex lock and acquires the lock */
mach_kernel:::mutex_lock_acquire
/execname == "noahtest3"/
{
    this->wait_time = timestamp - lock_wait_start[arg0];
    @waiting_times[arg1] = quantize(this->wait_time);
    lock_wait_start[arg0] = 0;
}

/* Probe when a thread releases a mutex lock */
mach_kernel:::mutex_unlock
/execname == "noahtest3"/
{
    lock_wait_start[arg0] = 0;
}

dtrace:::END
{
    printf("Lock wait times:\n");
    printa(@waiting_times);
}
