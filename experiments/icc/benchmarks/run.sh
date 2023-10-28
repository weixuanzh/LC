#!/bin/bash

# exit when any command fails
set -e
# import the the script containing common functions
source ../../include/scripts.sh

TASKS=("mbw.slurm")
sbatch_path=$(realpath "${sbatch_path:-.}")
build_path=$(realpath "${exe_path:-init/build/}")

if [[ -d "${build_path}" ]]; then
  echo "Run LCI benchmarks at ${exe_path}"
else
  echo "Did not find benchmarks at ${exe_path}!"
  exit 1
fi

# create the ./run directory
mkdir_s ./run
cd run

# setup module environment
module purge
module load cmake/3.18.4
module load openmpi

# --constraints="m256G&E2690V3&NoGPU&IB&HDR&E2690V3_IB_256G_NoGPU"
for i in $(eval echo {1..${1:-1}}); do
  for task in "${TASKS[@]}"; do
    sbatch ${sbatch_path}/${task} ${build_path}  || { echo "sbatch error!"; exit 1; }
  done
done
cd ..
