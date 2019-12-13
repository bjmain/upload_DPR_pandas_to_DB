# This script makes shell scripts for each pesticide use year that can be submitted to SBATCH.

# I then submit all the shell scripts with a bash command like: for D in #_uploader.sh; do sbatch $D; done

# NOTE: If you do not have a SLURM cluster, I would make a shell script (e.g. upload.sh) with: a command for each year like: 
#python pandas_to_postgres_DB.py pur2002
#python pandas_to_postgres_DB.py pur2003
# ... etc
# then run upload.sh with parallel on a detached screen like: parallel --gnu --ungroup --nice 9 -a upload.sh


# update this list with directories you want to upload
# NOTE: If you change the path, make sure you also update the name variable to make sure the path is stripped from the directory name
DPR_directories = ['../pur1990', '../pur1991', '../pur1992', '../pur1993', '../pur1994', '../pur1995', '../pur1996', '../pur1997', '../pur1998', '../pur1999', '../pur2000', '../pur2001', '../pur2002', '../pur2003', '../pur2004', '../pur2005', '../pur2006', '../pur2007', '../pur2008', '../pur2009', '../pur2010', '../pur2011', '../pur2012', '../pur2013', '../pur2014', '../pur2015', '../pur2016', '../pur2017']

for DIR in DPR_directories:
    name = DIR.strip("../pur")
    outfile = open("%s_uploader.sh" % (name), "w") 
    # uploader.sh is a template file
    for line in open("uploader.sh"):
        if "job-name" in line:
            new="#SBATCH --job-name='"+name+"'"
            outfile.write(new)
            outfile.write("\n")
            continue
        if not line.strip():
            outfile.write(" ")
            outfile.write("\n")
            continue
        if "cd" in line:
            outfile.write("cd /home/bmain/pesticide/upload_DPR_pandas_to_DB")
            outfile.write("\n")
            continue
        if "python" in line:
            outfile.write("python pandas_to_postgres_DB.py %s" % (DIR))
            outfile.write("\n")
            continue
        else:
            outfile.write(line)
    outfile.close()
