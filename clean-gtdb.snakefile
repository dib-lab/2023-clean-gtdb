
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
        sourmash sig manifest -o {output} {input}
    '''

rule gtdb_manifest_creation:
    input: "manifest/sourmash.manifest.original.csv"
    output: "manifest/gtdb.manifest.csv"
    benchmark:
        "benchmarks/gtdb_manifest_creation.txt"
    resources:
        mem_mb = 1000,
        time_min = 30
    shell:'''
        sed '1,2d' {input} | sed 's/^[^"]*"//; s/".*//' | cut -d' ' -f1 > {output} 
    '''


rule grepping_the_problem:
    input: db = "db/assembly_summary_genbank.txt",
           manifest1 = "manifest/gtdb.manifest.csv",
           manifest2 = "manifest/sourmash.manifest.original.csv"
    output: tmp1 = "manifest/all.gtdb.genbank.txt",
            tmp2 = "manifest/acc.gtdb.genbank.txt",
            tmp3 = "manifest/short.acc.gtdb.genbank.txt",
            clean = "manifest/sourmash.manifest.cleaned.csv"
    benchmark:
        "benchmarks/grepping_the_problem.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        grep -wFf {input.manifest1} {input.db} > {output.tmp1}
        cut -f 1 {output.tmp1} > {output.tmp2}
        sed 's/.\\{{4\\}}\\(.*\\)/\\1/' {output.tmp2} > {output.tmp3}
        grep -f {output.tmp3} {input.manifest2} > {output.clean}
    '''


rule picklist_picnic:
    input: origin = "manifest/sourmash.manifest.original.csv",
           clean = "manifest/sourmash.manifest.cleaned.csv",
           db = "db/gtdb-rs207.genomic-reps.dna.k31.zip"
    output: headers = "manifest/sourmash.manifest.csv",
            woohoo = "db/truncated.database.zip"
    conda: "envs/sourmash.yml"
    benchmark:
         "benchmarks/picklist_picninc.txt"
    resources:
        mem_mb = 8000,
        time_min = 30
    shell:'''
        ( head -n 2 {input.origin} && cat {input.clean} ) > {output.headers}
        sourmash sig extract --picklist {output.headers}:md5:md5 {input.db} -o {output.woohoo}
    '''
