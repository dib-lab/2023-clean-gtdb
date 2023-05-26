# 2023-clean-gtdb
An experimental snakemake workflow repository to remove suppressed or replaced genomes from sourmash workflow.

## Introduction

This workflow uses sourmash's [`manifest`](https://sourmash.readthedocs.io/en/latest/command-line.html#id48) and [`picklist`](https://sourmash.readthedocs.io/en/latest/command-line.html#id54) to exclude supressed and replaced genomes from the [prepared GTDB database](https://sourmash.readthedocs.io/en/latest/databases.html) of sourmash. 

## Operation

To install this workflow:
```
git clone https://github.com/dib-lab/2023-clean-gtdb.git
```
or
```
git clone git@github.com:dib-lab/2023-clean-gtdb.git
```

```
cd 2023-clean-gtdb
conda env create --name clean-db --file envs/environment.yml
conda activate clean-db
```

To run this workflow:
```
snakemake -s clean-gtdb.snakefile --use-conda --rerun-incomplete -j 1
```
or
```
snakemake -s clean-gtdb.snakefile -j 3 --use-conda --rerun-incomplete --resources mem_mb=12000 --cluster "sbatch -t {resources.time_min} -J clean-gtdb -p bmm -n 1 -N 1 -c {threads} --mem={resources.mem_mb}" -k
```

## Visualizations

![](https://raw.githubusercontent.com/dib-lab/2023-clean-gtdb/fix_faster/images/clean-gtdb-rulegraph.svg)

```
2023-clean-gtdb
├── LICENSE
├── README.md
├── benchmarks
│   ├── 0_download_all_available_genbank_genomes.txt
│   ├── 1_manifest_from_gtdb-rs207.genomic-reps.dna.k31.txt
│   ├── 2_get_ass_identifiers.txt
│   ├── 3_run_fun_script_on_gtdb-rs207.genomic-reps.dna.k31.txt
│   └── 4_picklist_picnic_gtdb-rs207.genomic-reps.dna.k31.txt
├── clean-gtdb.snakefile
├── db
│   ├── assembly_summary_genbank.txt
│   ├── gtdb-rs207.genomic-reps.dna.k31.clean.zip
│   └── gtdb-rs207.genomic-reps.dna.k31.zip
├── envs
│   ├── environment.yml
│   └── sourmash.yml
├── images
│   ├── clean-gtdb-dag.svg
│   ├── clean-gtdb-rulegraph.svg
│   ├── clean-gtdb-tree.html
│   └── clean-gtdb-tree.svg
├── manifest
│   ├── assembly_summary.ident.txt
│   ├── gtdb-rs207.genomic-reps.dna.k31.clean-report.txt
│   ├── gtdb-rs207.genomic-reps.dna.k31.mf.clean.csv
│   └── gtdb-rs207.genomic-reps.dna.k31.mf.csv
└── scripts
    └── munge-mf-with-idents.py

7 directories, 22 files
```

