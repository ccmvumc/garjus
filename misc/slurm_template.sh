#!/bin/bash
#SBATCH --account=ACCOUNT
#SBATCH -o OUTPUTDIR....ASSESSORlog.txt

uname -a # outputs node info (name, date&time, type, OS, etc)
date


#=============================================================================
INLIST=(

)

OUTLIST=(

)

JOBDIR=$(mktemp -d "/tmp/ASSESSOR.XXXXXXXXX") || { echo "mktemp failed"; exit 1; }

INDIR=$JOBDIR/INPUTS

OUTDIR=$JOBDIR/OUTPUTS

DSTDIR=ouptput.

CONTAINERPATH=...

MAINCMD="singularity \
exec \
--contain \
--cleanenv \
--home $JOBDIR \
--bind $INDIR:/INPUTS \
--bind $OUTDIR:/OUTPUTS \
--bind $JOBDIR:/tmp \
--bind $JOBDIR:/dev/shm \
$CONTAINERPATH \
bash -c \""
#=============================================================================



# Set up working dir structure
mkdir -p $INDIR
mkdir -p $OUTDIR

# Collect inputs
for IN in "${INLIST[@]}"; do
    copy
done

# Run main command
echo $MAINCMD
eval $MAINCMD

# Handle outputs
mkdir -p $DSTDIR
sha256sum $CONTAINERPATH | awk '{print $1}' > $DSTDIR/dax_docker_version.txt
errors=0
for OUT in "${OUTLIST[@]}"; do
    IFS=',' read -r col1 col2 col3 col4 <<< "$OUT"
    pathlist=(${OUTDIR}/${col1})
    
    # Check for missing output
    if [[ ${#pathlist[@]} == 1 && ! -e $pathlist ]]; then
        if [[ $col4 != "F" ]]; then errors=1 && echo "ERROR:missing required output:$col1"
        else echo "WARN:output not required"; fi
        continue
    fi

    # Copy output based on type
    mkdir -p "$DSTDIR/$col3"
    if [ $col3 == "PDF" ]; then
        if [ $col2 != "FILE" ]; then errors=1 && echo "ERROR:illegal type for PDF";
        elif [[ ${#pathlist[@]} != 1 ]]; then errors=1 && echo "ERROR:multiple PDFs";
        else cp $OUTDIR/$col1 $DSTDIR/$col3 || errors=1; fi
    elif [ $col2 == "FILE" ]; then cp $OUTDIR/$col1 $DSTDIR/$col3 || errors=1;
    elif [ $col2 == "DIR" ]; then cp -r $OUTDIR/$col1/. $DSTDIR/$col3 || errors=1;
    else errors=1 && echo "ERROR:invalid type:$col2"; fi
done

if [ $errors -gt 0 ]; then echo "JOB_FAILED" && touch $DSTDIR/JOB_FAILED.txt;
else echo "COMPLETE" && touch $DSTDIR/READY_TO_UPLOAD.txt; fi

rm -rf $INDIR $OUTDIR

echo "DONE!"
