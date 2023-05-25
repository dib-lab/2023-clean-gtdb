#! /usr/bin/env python
import sys
import argparse
import csv

import sourmash
from sourmash import manifest # also does 'import sourmash'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('good_idents', help='list of valid identifiers')
    p.add_argument('old_mf', help='existing sourmash database manifest')
    p.add_argument('--report', help='details of removed etc., for hoomans')
    p.add_argument('-o', '--output',
                   help='manifest cleansed of the impure')
    args = p.parse_args()

    good_idents = set()
    good_idents_no_version = set()

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

    print(f"\n\nLoaded {len(good_idents)} genbank identifiers from '{args.good_idents}'",
          file=sys.stderr)
    print(f"(and loaded {len(good_idents_no_version)} identifiers without version number)", file=sys.stderr)
    assert len(good_idents) == len(good_idents_no_version)

    old_mf = manifest.BaseCollectionManifest.load_from_filename(args.old_mf)

    ## now go through and filter

    removed_list = []
    updated_version_list = []

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

    n_removed = len(old_mf) - len(keep_rows)
    n_suspect_suspension = n_removed - n_changed_version
    new_mf = manifest.CollectionManifest(keep_rows)
    
    print(f"\nFrom '{args.old_mf}':", file=sys.stderr)
    print(f"Kept {len(keep_rows)} of {len(old_mf)} identifiers.",
            file=sys.stderr)
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
