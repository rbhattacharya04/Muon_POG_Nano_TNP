# Muon POG nanoAOD Tag&Probe tool

This tool is intended to be plugin within the spark_tnp framework [https://gitlab.cern.ch/cms-muonPOG/spark_tnp]. It takes as input Muon nanoAOD files and return parquet files with Tag muon and Probe muon pair ntuples.

## Install

Install the environment:

```
git clone ....
./install
```

Once ready, setup the environment everytime you wish to run the code:

```
source start.sh
```

## Run TnP workflow

The configuration file Muon_TnP_cfg.py needs to be fill everytime you want to register a new production campaign. Then, a few options are considered.

```
**Options**
-p (--prod)     PRODUCTION NAME
-s (--doSubmit) SUBMIT JOBS TO CONDOR (0 or 1)
-F (--selFile)  RUN ON ONE SINGLE FILE
-dR (--dryRun)  DRY RUN / DO NOT SUBMIT (0 or 1)
```

**Split and submit jobs to HTCondor (Recommended):**

```
python TNP_Muon_POG.py -p PRODUCTION -s 1 
```

Run the code interactively:

```
python TNP_Muon_POG.py -p PRODUCTION 
```

The code returns one parquet file per ROOT input file. They can be merged easly in hadoop using the spark_tnp tool. Once there, you can setup the input and output directories and run:

```
python doMerge.py
```

# Time consumption

## First step: TnP ntuple creation

```
Time per job: < 2 minutes
```

## Second step: Merge files

Should depend on the batch size. First attemp using 100 files.

```
Time to merge: ~5 minutes
```







