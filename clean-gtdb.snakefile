
rule all:
    input: "db/truncated.database.zip"

rule download_sourmash_gather_database:
    output: "db/gtdb-rs207.genomic-reps.dna.k31.zip"
    threads: 1
    benchmark:
        "benchmarks/download_sourmash_gather_database.txt"
    resources:
        mem_mb = 1000,
        time_min = 30
    shell:'''
        wget -O {output} https://farm.cse.ucdavis.edu/~ctbrown/sourmash-db/gtdb-rs207/gtdb-rs207.genomic-reps.dna.k31.zip
    '''

rule download_all_available_genbank_genomes:
    output: "db/assembly_summary_genbank.txt"
    threads: 1
    benchmark:
        "benchmarks/download_all_available_genbank_genomes.txt"
    resources:
        mem_mb = 1000,
        time_min = 30
    shell:'''
        rsync -t -v rsync://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt {output}
    '''

rule sourmash_manifest_destiny:
    input: "db/{db}.zip",
    output: "manifest/{db}.mf.csv",
    conda: "envs/sourmash.yml"
    benchmark:
        "benchmarks/{db}.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        sourmash sig manifest -o {output} {input} --no-rebuild
    '''

rule get_assembly_summary_identifiers:
    input:
        db = "db/assembly_summary_genbank.txt",
    output: 
        tmp = "manifest/assembly_summary.ident.txt",
    benchmark:
        "benchmarks/grepping_the_problem.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        cut -f 1 {input.db} | awk 'NR>2' > {output.tmp}
    '''

rule run_fun_script:
    input:
        summary_idents = "manifest/assembly_summary.ident.txt",
        mf = "manifest/gtdb-rs207.genomic-reps.dna.k31.mf.csv",
    output:
        new_mf = "manifest/sourmash.manifest.clean.csv",
        report = "manifest/clean-report.txt"
    shell: """
        ./munge-mf-with-idents.py {input.summary_idents} {input.mf} \
            --report {output.report} -o {output.new_mf}
    """


rule picklist_picnic:
    input:
        clean = "manifest/short.acc.gtdb.genbank.txt",
        db = "db/gtdb-rs207.genomic-reps.dna.k31.zip"
    output:
        woohoo = "db/truncated.database.zip"
    conda: "envs/sourmash.yml"
    benchmark:
         "benchmarks/picklist_picnic.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        sourmash sig extract --picklist {input.clean}:ident:ident {input.db} -o {output.woohoo}
    '''
