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

    print(f"loaded {len(good_idents)} identifiers from '{args.good_idents}'",
          file=sys.stderr)
    print(f"(and loaded {len(good_idents_no_version)} identifiers sans version #)", file=sys.stderr)
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
    print(f"Kept {len(keep_rows)} from {len(old_mf)}; removed {n_removed} total",
          file=sys.stderr)
    new_mf = manifest.CollectionManifest(keep_rows)

    with open(args.output, 'w', newline='') as outfp:
        new_mf.write_to_csv(outfp, write_header=True)

    print(f"Kept {len(new_mf)}.", file=sys.stderr)
    print(f"Removed {n_removed} total.", file=sys.stderr)
    print(f"Removed {n_changed_version} because of changed version",
          file=sys.stderr)
    if args.report:
        with open(args.report, 'wt') as fp:
            print(f"Kept {len(new_mf)}.", file=fp)
            print(f"Removed {n_removed} total.", file=fp)
            print(f"Removed {n_changed_version} because of changed version",
                  file=fp)
            print("---- removed because presumed guilty ----", file=fp)
            print("\n".join(removed_list), file=fp)
            print("---- removed because version changed ----", file=fp)
            print("\n".join(updated_version_list), file=fp)


if __name__ == '__main__':
    sys.exit(main())
