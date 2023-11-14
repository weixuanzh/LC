#!/bin/bash

echo "perf record --freq=99 --call-graph dwarf -q -o perf.data.$SLURM_JOB_ID.$SLURM_PROCID ${ROOT_PATH}/init/build/benchmarks/lcitb_pt2pt --op 1l --nthreads 64 --thread-pin 1 --send-comp-type=sync --send-reg 1 --max-msg-size 1048576 --nsteps 1000"
perf record --freq=99 --call-graph dwarf -q -o perf.data.$SLURM_JOB_ID.$SLURM_PROCID ${ROOT_PATH}/init/build/benchmarks/lcitb_pt2pt --op 1l --nthreads 64 --thread-pin 1 --send-comp-type=sync --send-reg 1 --max-msg-size 1048576 --nsteps 1000