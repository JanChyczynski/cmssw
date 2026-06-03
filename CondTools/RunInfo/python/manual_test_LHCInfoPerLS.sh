#!/bin/bash
# exit on error
set -e

# ----- ARGUMENTS
MODE="endFill"
# START="2026-05-31 15:43:20.000"
START="2026-06-02 18:43:20.000"
END="2026-06-03 15:43:20.000"

TESTNAME="writeManyOff_timev1_detailed"
TEST_NR="2"

LAST_LUMI_OVERRIDE="1737640688746590"
TAG="LHCInfoPerFill_test"

if [ "$MODE" == "duringFill" ]; then
    TESTNAME="duringF_${TESTNAME}"
else
    TESTNAME="endF_${TESTNAME}"
fi


# TEST SETUP
TESTFILE="${TESTNAME}_${TEST_NR}"

SRCDIR=/eos/home-j/jchyczyn/alca/phase2/17_0_0_pre1_time_O2O/src/CondTools/RunInfo/python
TESTDIR=/eos/home-j/jchyczyn/alca/phase2/17_0_0_pre1_time_O2O/src/CondTools/RunInfo/python/test1
mkdir -p $TESTDIR

LOGFILE=$TESTDIR/${TESTFILE}_$(date +%Y-%m-%d_%Hh%Mm%S).log
DEST_DB="sqlite_file:${TESTDIR}/${TESTFILE}.db"

#if exists ask if it should be removed
if [ -f $TESTDIR/${TESTFILE}.db ]; then
    read -p "File ${TESTDIR}/${TESTFILE}.db already exists. Do you want to remove it? (y/n) " -n 1 -r
    echo    # move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm $TESTDIR/${TESTFILE}.db
        echo "File ${TESTDIR}/${TESTFILE}.db removed." | tee -a $LOGFILE
    else
        echo "File ${TESTDIR}/${TESTFILE}.db exists and was not removed." | tee -a $LOGFILE
    fi
fi

echo "${LAST_LUMI_OVERRIDE}" > ${TESTDIR}/${TESTFILE}_lastLumi.txt

#deployment vars
XANGLE_MIN="-500"
THROW=True
DEFAULT_XANGLE_X="0"
DEFAULT_XANGLE_Y="-160"
DEFAULT_BETA_X="1.2"
DEFAULT_BETA_Y="1.2"



# ------- EXECUTION
echo "writing to log file: $LOGFILE"
if [ "$MODE" == "duringFill" ]; then
    { time cmsRun $SRCDIR/LHCInfoPerLSPopConAnalyzer_cfg.py mode=duringFill destinationConnection=$DEST_DB tag=$TAG \
        lastLumiFile=${TESTDIR}/${TESTFILE}_lastLumi.txt \
        minCrossingAngle=$XANGLE_MIN throwOnInvalid=$THROW defaultXangleX=$DEFAULT_XANGLE_X defaultXangleY=$DEFAULT_XANGLE_Y defaultBetaX=$DEFAULT_BETA_X defaultBetaY=$DEFAULT_BETA_Y \
        startTime="$START"; } \
        2>&1 | tee -a $LOGFILE
elif [ "$MODE" == "endFill" ]; then
    { time cmsRun $SRCDIR/LHCInfoPerLSPopConAnalyzer_cfg.py mode=endFill destinationConnection=$DEST_DB tag=$TAG \
        startTime="$START" endTime="$END"; } \
        2>&1 | tee -a $LOGFILE
else
    echo "Invalid mode: $MODE. Use 'duringFill' or 'endFill'."
    exit 1
fi

echo "wrote to log file: $LOGFILE"




