#!/usr/bin/env bash

set -o errexit

pkgv=$(cargo pkgid | awk -F'[ #]' '{print "v"$2}')

ghv=$(gh release list -L 1 | awk -F" " '{print $1}')

# compare versions
if [[ $pkgv != $ghv ]]; then
    echo "'$pkgv' != '$ghv' ... creating new release tag"
    # create new rel tag
    url=$(gh release create "$pkgv"  --repo="$GITHUB_REPOSITORY" --title="$pkgv" --target="$GITHUB_SHA" --generate-notes)
    echo "new release created at: $url"
    echo "created=1" >> $GITHUB_OUTPUT
else
    echo "$pkgv == $ghv ... stopping"
    echo "created=0" >> $GITHUB_OUTPUT
fi
