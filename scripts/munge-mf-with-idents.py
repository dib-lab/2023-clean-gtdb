#! /usr/bin/env python
import sys
import argparse
import csv
import os.path
import time
import sourmash
from sourmash import manifest # also does 'import sourmash'


def main():
    p = argparse.ArgumentParser()

    # Add command-line arguments with default values from Snakemake
    p.add_argument('good_idents', nargs='?', default=snakemake.input.summary_idents, help='list of valid identifiers')
    p.add_argument('bad_idents', nargs='?', default=snakemake.input.supressed_idents, help='list of suppressed identifiers')
    p.add_argument('old_mf', nargs='?', default=snakemake.input.mf, help='existing sourmash database manifest')
    p.add_argument('--report', nargs='?', default=snakemake.output.report, help='details of removed etc., for humans')
    p.add_argument('-o', '--output', nargs='?', default=snakemake.output.new_mf, help='manifest cleansed of the impure')
    args = p.parse_args()

    good_idents = set()
    good_idents_no_version = set()

    bad_idents = set()
    bad_idents_no_version = set()

    def get_suffix(name):
        ident = name.split(' ')[0]
        assert ident.startswith('GC')
        suffix = ident[3:]
        return suffix

    def get_suffix_no_version(name):
        ident = name.split(' ')[0]
        suffix = get_suffix(ident)
        assert '.' in suffix
        return suffix.split('.')[0]

    with open(args.good_idents, 'rt') as fp:
        for line in fp:
            assert line.startswith('GC')
            line = line.strip()
            suffix = get_suffix(line)
            good_idents.add(suffix)

            suffix_no_version = get_suffix_no_version(line)
            good_idents_no_version.add(suffix_no_version)

    with open(args.bad_idents, 'rt') as fp:
        for line in fp:
            assert line.startswith('GC')
            line = line.strip()
            suffix = get_suffix(line)
            bad_idents.add(suffix)

            suffix_no_version = get_suffix_no_version(line)
            bad_idents_no_version.add(suffix_no_version)

    assert len(good_idents) == len(good_idents_no_version)

    #assert len(bad_idents) == len(bad_idents_no_version) This breaks the script! Why?
    print(len(bad_idents_no_version))
    print(len(bad_idents))
    old_mf = manifest.BaseCollectionManifest.load_from_filename(args.old_mf)

    ## now go through and filter

    removed_list = []
    updated_version_list = []
    bad_list = []

    keep_rows = []
    n_changed_version = 0
    for row in old_mf.rows:
        name = row['name']
        suffix = get_suffix(name)
        suffix_no_version = get_suffix_no_version(name)

        if suffix in good_idents:
            keep_rows.append(row)
        else:
            if suffix_no_version in good_idents_no_version:
                n_changed_version += 1
                updated_version_list.append(name)
            else:
                removed_list.append(name)
        
        if suffix in bad_idents:
            bad_list.append(row)


    n_removed = len(old_mf) - len(keep_rows)
    n_suspect_suspension = n_removed - n_changed_version
    new_mf = manifest.CollectionManifest(keep_rows)
    
    #genbank_time = snakemake@params[['genbank_time']]
    genbank_time = time.ctime(os.path.getmtime(snakemake.params.genbank_time)) 


    print(f"\n\nFrom current GenBank genome assemblies:", file=sys.stderr)
    print(f"Loaded {len(good_idents)} genbank identifiers from '{args.good_idents}'",
                          file=sys.stderr)
    print(f"(and loaded {len(good_idents_no_version)} identifiers without version number)", file=sys.stderr)
    print(f"File last modified {genbank_time}", file=sys.stderr)

    print(f"\nFrom '{args.old_mf}':", file=sys.stderr)
    print(f"Kept {len(keep_rows)} of {len(old_mf)} identifiers.",
            file=sys.stderr)
    
    print(f"\nFrom '{args.bad_idents}':", file=sys.stderr)
    print(f"Kept {len(bad_list)} of {len(bad_idents)} identifiers.", file=sys.stderr)
    
    print(f"\nNew manifest '{args.output}':", file=sys.stderr)
    print(f"Kept {len(new_mf)} identifiers.", file=sys.stderr)
    print(f"Removed {n_removed} total identifiers.",
            file=sys.stderr)
    print(f"Removed {n_changed_version} identifiers because of version change.",
            file=sys.stderr)
    print(f"Removed {n_suspect_suspension} identifiers because of suspected suspension of the genome.\n\n",
            file=sys.stderr)


    with open(args.output, 'w', newline='') as outfp:
        new_mf.write_to_csv(outfp, write_header=True)

    if args.report:
        with open(args.report, 'wt') as fp:
            print(f"From '{args.old_mf}':", file=fp)
            print(f"Kept {len(new_mf)}.", file=fp)
            print(f"Removed {n_removed} total.", file=fp)
            print(f"Removed {n_suspect_suspension} identifiers because of suspected suspension of the genome.",
                  file=fp)
            print(f"Removed {n_changed_version} because of changed version.",
                  file=fp)
            print(f"---- {n_suspect_suspension} removed because presumed guilt ----", file=fp)
            print("\n".join(removed_list), file=fp)
            print(f"---- {n_removed} removed because version changed ----", file=fp)
            print("\n".join(updated_version_list), file=fp)


if __name__ == '__main__':
    sys.exit(main())
