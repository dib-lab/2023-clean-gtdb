
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
    input: "db/gtdb-rs207.genomic-reps.dna.k31.zip"
    output: "manifest/sourmash.manifest.original.csv"
    conda: "envs/sourmash.yml"
    benchmark:
        "benchmarks/sourmash_manifest_destiny.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        sourmash sig manifest -o {output} {input} --no-rebuild
    '''

rule grepping_the_problem:
    input:
        db = "db/assembly_summary_genbank.txt",
    output: 
        tmp = "manifest/short.acc.gtdb.genbank.txt",
    benchmark:
        "benchmarks/grepping_the_problem.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        {{ echo ident; cut -f 1 {input.db} | awk 'NR>2'; }} > {output.tmp}
    '''


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
