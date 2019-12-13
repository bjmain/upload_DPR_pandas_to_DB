#!/bin/bash

# options for sbatch
#SBATCH --nodes=1
#SBATCH --partition=bmm
#SBATCH --time=3800
#SBATCH --mem=10G # Memory pool for all cores (see also --mem-per-cpu)
#SBATCH --job-name="2017"

begin=`date +%s`
echo $HOSTNAME

# setting up variables
#sample=$1
#R1=${sample}_L006_R1_001.fastq.gz
#R2=${sample}_L006_R2_001.fastq.gz

# loading modules
#module load maker

# running commands
cd /home/bmain/pesticide/pur2017
python insert_DF_2_gateway2.py

# finished commands

# getting end time to calculate time elapsed
end=`date +%s`
elapsed=`expr $end - $begin`
echo Time taken: $elapsed

