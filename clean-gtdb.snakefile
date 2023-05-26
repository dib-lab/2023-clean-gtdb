# must use 
# snakemake -s clean-gtdb.snakefile -j 1 --use-conda 
# at minimum 
#
# could include a config file for easy changing of databases instead of the list below?



DB_V=['gtdb-rs207.genomic-reps.dna.k31'] #add any number of gtdb sourmash databases to this list to process them all. but line 13?

#configfile: "config.yml"

rule all:
    input: expand("db/{gtdb}.clean.zip", gtdb=DB_V)

rule download_sourmash_gather_database:
    output: expand("db/{db}.zip", db=DB_V)
    threads: 1
    resources:
        mem_mb = 1000,
        time_min = 30
    shell:'''
        wget -O {output} https://farm.cse.ucdavis.edu/~ctbrown/sourmash-db/gtdb-rs207/gtdb-rs207.genomic-reps.dna.k31.zip
    '''

rule download_all_available_genbank_genomes:
    output: 
        full_db = "db/assembly_summary_genbank.txt",
        supressed_db = "db/assembly_summary_genbank_historical.txt",
    threads: 1
    benchmark:
        "benchmarks/0_download_all_available_genbank_genomes.txt"
    resources:
        mem_mb = 1000,
        time_min = 30
    shell:'''
        rsync -t -v rsync://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt {output.full_db}
        rsync -t -v rsync://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank_historical.txt {output.supressed_db}
    '''

rule sourmash_manifest_destiny:
    input: "db/{db}.zip",
    output: "manifest/{db}.mf.csv",
    conda: "envs/sourmash.yml"
    benchmark:
        "benchmarks/1_manifest_from_{db}.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        sourmash sig manifest -o {output} {input} --no-rebuild
    '''

rule get_assembly_summary_identifiers:
    input:
        db_good = "db/assembly_summary_genbank.txt",
        db_bad = "db/assembly_summary_genbank_historical.txt",
    output: 
        good = "manifest/assembly_summary_genbank.good_ident.txt",
        bad = "manifest/assembly_summary_genbank_historical.bad_ident.txt",
    benchmark:
        "benchmarks/2_get_ass_identifiers.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        cut -f 1 {input.db_good} | awk 'NR>2' > {output.good}
        cut -f 1 {input.db_bad} | awk 'NR>2' > {output.bad}
    '''

rule run_fun_script:
    input:
        summary_idents = "manifest/assembly_summary_genbank.good_ident.txt",
        supressed_idents = "manifest/assembly_summary_genbank_historical.bad_ident.txt",
        mf = "manifest/{db}.mf.csv",
        script = "scripts/munge-mf-with-idents.py",
    output:
        new_mf = "manifest/{db}.mf.clean.csv",
        report = "manifest/{db}.clean-report.txt"
    conda: "envs/sourmash.yml"
    benchmark:
        "benchmarks/3_run_fun_script_on_{db}.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    params: genbank_time="db/assembly_summary_genbank.txt"
        #genbank_time="last modified: %s" % time.ctime(os.path.getmtime("db/assembly_summary_genbank.txt"))
    script: "scripts/munge-mf-with-idents.py"

#   shell: """
#       {input.script} {input.summary_idents} {input.mf} \
#           --report {output.report} -o {output.new_mf}
#    """


rule picklist_picnic:
    input:
        clean = "manifest/{db}.mf.clean.csv",
        db = "db/{db}.zip"
    output:
        woohoo = "db/{db}.clean.zip"
    conda: "envs/sourmash.yml"
    benchmark:
         "benchmarks/4_picklist_picnic_{db}.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        sourmash sig extract --picklist {input.clean}::manifest {input.db} -o {output.woohoo}
    '''
